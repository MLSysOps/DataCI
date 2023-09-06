#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Jun 11, 2023
"""
import ast
import inspect
import os.path
import re
import shutil
import subprocess
import sys
from collections import defaultdict
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING

import networkx as nx
from airflow.api.client.local_client import Client
from airflow.models import DAG as _DAG
from airflow.operators.python import PythonOperator as _PythonOperator

from dataci.models import Workflow, Stage, Dataset
from dataci.plugins.orchestrator.script import locate_main_block, get_source_segment, \
    locate_dag_function
from dataci.server.trigger import Trigger as _Trigger, EVENT_QUEUE, QUEUE_END
from dataci.utils import hash_file

if TYPE_CHECKING:
    from typing import Any, List


class DAG(Workflow, _DAG):
    """A wrapper class for :code:`airflow.models.DAG`. Substituting the
    :code:`airflow.models.DAG` class with this class to provide version control
    over the DAGs.
    """
    BACKEND = 'airflow'
    _script_tempdir_registry: 'List[TemporaryDirectory]' = list()

    def __init__(self, dag_id, *args, **kwargs):
        # If dag id is overridden by workspace--name--version, extract the name
        if re.match(r'\w+--\w+--[\da-f]+', dag_id):
            name = dag_id.split('--')[1]
        else:
            name = dag_id
        # Preserve the script tempdir to prevent it from auto cleanup
        self.__script_tempdir = None
        super().__init__(name, *args, dag_id=dag_id, **kwargs)

    @property
    def stages(self):
        return self.tasks

    @property
    def dag(self):
        g = nx.DiGraph()
        for t in self.tasks:
            g.add_node(t)
            g.add_edges_from([(t, d) for d in t.downstream_list])
        return g

    @property
    def backend_id(self):
        return f'{self.workspace.name}--{self.dag_id}--{self.version_tag}'

    @property
    def input_datasets(self):
        dataset_names = set()
        for stage in self.stages:
            if isinstance(stage, Stage):
                # Only add input table names
                for v in stage.input_table.values():
                    dataset_names.add(v['name'] if isinstance(v, dict) else v)
        # Remove Ellipsis
        dataset_names.discard(...)
        return map(Dataset.get, dataset_names)

    @property
    def output_datasets(self):
        # TODO: get dag output datasets
        raise NotImplementedError

    @property
    def script(self):
        # TODO: pack the multiple scripts into a zip file
        if self._script_dir is None:
            self._script_dir = TemporaryDirectory(dir=self.workspace.tmp_dir)
            target_dir = Path(self._script_dir.name)
            entrypoint = Path(self.fileloc)
            root_dir = entrypoint.parent
            self._entrypoint = entrypoint.relative_to(root_dir).as_posix()

            # Copy everything within the root dir
            shutil.copytree(root_dir, target_dir, dirs_exist_ok=True)
            # Scan dir
            file_checksums, filemap = dict(), defaultdict(list)
            for abs_file_name in target_dir.glob('**/*'):
                if abs_file_name.is_dir():
                    continue
                rel_file_name = abs_file_name.relative_to(target_dir).as_posix()
                file_checksums[rel_file_name] = hash_file(abs_file_name)
                filemap[rel_file_name].append('workflow')

            # Copy the script content of each stage to the workflow script dir
            for stage in self.stages:
                stage_root_dir = Path(stage.script['path'])
                for abs_file_name in stage_root_dir.glob('**/*'):
                    if abs_file_name.is_dir():
                        continue
                    rel_file_name = abs_file_name.relative_to(stage_root_dir).as_posix()
                    file_checksum = hash_file(abs_file_name)
                    if file_checksums.get(rel_file_name, file_checksum) != file_checksum:
                        raise FileExistsError(
                            f'Stage {stage.identifier} script file {rel_file_name} has different content '
                            f'with other stages or workflow.\n'
                            f'Found conflict source file from: {filemap[rel_file_name]}\n'
                        )
                    # Copy to workflow script dir with relative path
                    if rel_file_name not in file_checksums:
                        shutil.copy2(abs_file_name, target_dir)
                        file_checksums[rel_file_name] = file_checksum
                        filemap[rel_file_name].append(stage.identifier)

        return super().script

    def publish(self):
        """Publish the DAG to the backend."""
        super().publish()
        # Copy the script content to the published file
        publish_dir = Path.home() / 'airflow' / 'dags' / self.workspace.name / self.name / self.version_tag
        # Clear dir if exists
        if publish_dir.exists():
            publish_dir.rmdir()
        shutil.copytree(self.script, publish_dir)

        # Adjust the workflow name in dag script (entry file)
        # Use workspace__name__version as dag id
        dag_script_path = publish_dir / self.entry_file
        script = dag_script_path.read_text()
        # FIXME: we need some trick for airflow to recognize the dag
        #     Add a commented line with import airflow dag, otherwise airflow will not scan the script
        script = '# Dummy line to trigger airflow scan\n' \
                 '# from airflow.decorator import dag\n' + \
                 script

        # Parse the dag id from @dag(...) call
        dag_nodes, deco_nodes = locate_dag_function(ast.parse(script), self.name)
        if len(dag_nodes) != 1:
            raise ValueError(
                f'@dag(...) decorator is not found or multiple @dag(...) decorators are '
                f'found in dag definition: \n{script}'
            )
        dag_node, deco_node = dag_nodes[0], deco_nodes[0]

        from ..decorators import dag as dag_func

        # get dag_id
        # TODO: use full syntax tree parser to get and modify the dag_id https://github.com/Instagram/LibCST
        dag_args, dag_kwargs = deco_node.args, {kwarg.arg: kwarg.value for kwarg in deco_node.keywords}
        bound = inspect.signature(dag_func).bind(*dag_args, **dag_kwargs)
        dag_id = ast.literal_eval(bound.arguments.get('dag_id', 'None'))
        deco_node.col_offset -= 1
        deco_code = get_source_segment(script, deco_node, padded=True)
        deco_node.col_offset += 1
        if dag_id is None:
            # Add dag_id="workspace--name--version" to @dag(...) call
            new_deco_code = re.sub(r'@dag\s*\(', f'@dag("{self.backend_id}", ', deco_code, 1)
        else:
            # Replace dag_id with workspace__name__version
            new_deco_code = deco_code.replace(dag_id, self.backend_id, 1)
        # replace @dag(...) call with @dag(dag_id="workspace__name__version", ...)
        dag_code_snippet = get_source_segment(script, dag_node, padded=True)
        new_dag_code_snippet = dag_code_snippet.replace(deco_code, new_deco_code, 1)
        script = script.replace(dag_code_snippet, new_dag_code_snippet, 1)

        # Remove the published file if exists
        with open(dag_script_path, 'w') as f:
            f.write(script)

        # Airflow re-serialize the dag file
        subprocess.call([sys.executable, '-m', 'airflow', 'dags', 'reserialize', '-S', str(publish_dir)])
        # Airflow unpause the dag
        subprocess.call([sys.executable, '-m', 'airflow', 'dags', 'unpause', self.backend_id])
        self.logger.info(f'Published workflow: {self}')

        return self

    def run(self, **kwargs):
        c = Client(None)
        self.logger.info(f'Manual run workflow: {self} (dag_id={self.backend_id})')
        response = c.trigger_dag(self.backend_id, **kwargs)
        return response['dag_run_id'] if response else None

    def test(self, **kwargs):
        super(Workflow, self).test(**kwargs)  # noqa

    def backfill(self, **kwargs):
        super(Workflow, self).run(**kwargs)  # noqa


class PythonOperator(Stage, _PythonOperator):
    name_arg = 'task_id'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._fileloc = inspect.getsourcefile(self.python_callable)
        self.__init_args = (*args, *kwargs.items())

    def execute_callable(self) -> 'Any':
        """Execute the python callable."""
        # Resolve input table
        bound = inspect.signature(self.python_callable).bind(*self.op_args, **self.op_kwargs)
        for arg_name, arg_value in self.input_table.items():
            if arg_name in bound.arguments:
                arg_value = arg_value if arg_value is not Ellipsis else bound.arguments[arg_name]
                if not isinstance(arg_value, dict):
                    # If the input table is not a dict, it is a dataset identifier
                    arg_value = {'name': arg_value}
                dataset = Dataset.get(**arg_value)
                bound.arguments[arg_name] = dataset.read()
                # For logging
                self.log.info(f'Input table {arg_name}: {dataset.identifier}')

        self.op_args, self.op_kwargs = bound.args, bound.kwargs
        # Run the stage by backend
        ret = super().execute_callable()
        # Save the output table
        for key, dataset in self.output_table.items():
            # If the operator has multiple outputs (get from airflow python operator)
            if self.multiple_outputs:
                dataset.dataset_files = ret[key]
                dataset.save()
                ret[key] = {
                    'name': dataset.identifier,
                    'file_reader': dataset.file_reader.NAME,
                    'file_writer': dataset.file_writer.NAME
                }
            else:
                dataset.dataset_files = ret
                dataset.save()
                ret = {
                    'name': dataset.identifier,
                    'file_reader': dataset.file_reader.NAME,
                    'file_writer': dataset.file_writer.NAME
                }
            self.log.info(f'Output table {key}: {dataset.identifier}')
        return ret

    def test(self, *args, **kwargs):
        # Provide context for operator args and kwargs
        self.op_args, self.op_kwargs = args, kwargs
        try:
            # Run the stage by backend
            return self.execute_callable()
        finally:
            # Restore the operator args and kwargs
            self.op_args, self.op_kwargs = args, kwargs

    @property
    def script(self):
        if self._script_dir is None:
            self._script_dir = TemporaryDirectory(dir=self.workspace.tmp_dir)
            self._entrypoint = os.path.basename(self._fileloc)
            # Copy to tempdir with relative path
            target_dir = os.path.join(self._script_dir.name, os.path.dirname(self._entrypoint))
            shutil.copy2(self._fileloc, target_dir)
        return super().script


class Trigger(_Trigger):
    def runner(self):
        while True:
            event = EVENT_QUEUE.get()
            if event is QUEUE_END:
                break
            self.logger.debug(f'Received event: {event}')
            workflow_identifiers = self.get(event)
            for workflow_identifier in workflow_identifiers:
                # Trigger airflow dag
                dag_id = workflow_identifier.replace('.', '--').replace('@', '--')
                self.logger.info(f'Event {event} trigger workflow: Workflow({workflow_identifier}) (dag_id={dag_id})')
                try:
                    c = Client(None)
                    response = c.trigger_dag(dag_id)
                    if response is not None:
                        self.logger.info(f'Triggered workflow run: {response["dag_id"]}.{response["dag_run_id"]}')
                except Exception as e:
                    self.logger.error(f'Failed to trigger workflow: {workflow_identifier} (dag_id={dag_id})')
                    self.logger.error(e)
