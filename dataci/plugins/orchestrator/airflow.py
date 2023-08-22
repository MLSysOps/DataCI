#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Jun 11, 2023
"""
import inspect
import re
import subprocess
import sys
from pathlib import Path
from textwrap import indent
from typing import TYPE_CHECKING

import networkx as nx
from airflow.api.client.local_client import Client
from airflow.models import DAG as _DAG
from airflow.operators.python import PythonOperator as _PythonOperator

from dataci.models import Workflow, Stage, Dataset
from dataci.plugins.orchestrator.utils import parse_func_params
from dataci.server.trigger import Trigger as _Trigger, EVENT_QUEUE, QUEUE_END

if TYPE_CHECKING:
    from typing import Any


class DAG(Workflow, _DAG):
    """A wrapper class for :code:`airflow.models.DAG`. Substituting the
    :code:`airflow.models.DAG` class with this class to provide version control
    over the DAGs.
    """
    BACKEND = 'airflow'

    def __init__(self, dag_id, *args, **kwargs):
        # If dag id is overridden by workspace--name--version, extract the name
        if re.match(r'\w+--\w+--[\da-f]+', dag_id):
            name = dag_id.split('--')[1]
        else:
            name = dag_id
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
        if self._script is None:
            with open(self.fileloc, 'r') as f:
                script_origin = f.read()
            # Remove stage import statements:
            # from xxx import stage_name / import stage_name
            stage_name_pattern = '|'.join([stage.name for stage in self.stages])
            import_pattern = re.compile(
                rf'^(?:from\s+[\w.]+\s+)?import\s+(?:{stage_name_pattern})[\r\t\f ]*\n', flags=re.MULTILINE
            )
            script = import_pattern.sub('', script_origin)

            # Insert the stage scripts before the DAG script:
            for stage in self.stages:
                stage_script = stage.script
                # Avoid double copy two stage within the same script
                if stage_script in script or stage_script in script_origin:
                    continue
                script = stage_script + '\n' * 2 + script

            # Remove the __main__ guard
            script = re.sub(
                r'if\s+__name__\s+==\s+(?:"__main__"|\'__main__\'):(\n\s{4}.*)+', '', script
            )

            self._script = script.strip()
        return self._script

    def publish(self):
        """Publish the DAG to the backend."""
        super().publish()
        # Copy the script content to the published file
        publish_file_path = (Path.home() / 'airflow' / 'dags' / self.backend_id).with_suffix('.py')
        # Create parent dir if not exists
        publish_file_path.parent.mkdir(parents=True, exist_ok=True)
        # Adjust the workflow name
        # Use workspace__name__version as dag id
        # Match @dag(...) pattern
        #    ([^\S\r\n]*) match group matches the indent before @dag(...)
        #    (?:[^()]*\([^()]*\))*  non-capture match group avoid nested round brackets in @dag(...) call
        #    ((?:[^()]*\([^()]*\))*[^()]*) match group matches everything in @dag(...) call
        dag_decorator_pattern = re.compile(r'([^\S\r\n]*)@dag\(((?:[^()]*\([^()]*\))*[^()]*)\)',
                                           flags=re.MULTILINE | re.DOTALL)

        script = self.script
        # FIXME: we need some trick for airflow to recognize the dag
        #     Add a commented line with import airflow dag, otherwise airflow will not scan the script
        script = '# Dummy line to trigger airflow scan\n' \
                 '# from airflow.decorator import dag\n' + \
                 script

        # Parse the dag id from @dag(...) call
        dag_decorator_match_grps = dag_decorator_pattern.findall(script)
        if len(dag_decorator_match_grps) != 1:
            raise ValueError(
                f'@dag(...) decorator is not found or multiple @dag(...) decorators are '
                f'found in dag definition: \n{script}'
            )

        from ..decorators import dag as dag_func

        dag_decorator_indent, dag_decorator_args = dag_decorator_match_grps[0]
        dag_args, dag_kwargs = parse_func_params(dag_decorator_args)
        # get dag_id
        bound = inspect.signature(dag_func).bind(*dag_args, **dag_kwargs)
        # replace dag_id with workspace__name__version
        bound.arguments['dag_id'] = f'"{self.backend_id}"'
        # replace @dag(...) call with @dag(dag_id="workspace__name__version", ...)
        dag_args, dag_kwargs = bound.args, bound.kwargs
        dag_decorator_args = indent(',\n'.join(
            dag_args + tuple(f'{arg_name}={arg_value}' for arg_name, arg_value in dag_kwargs.items())
        ), dag_decorator_indent + ' ' * 4)
        script = dag_decorator_pattern.sub(f'@dag(\n{dag_decorator_args}\n)', script)

        # Remove the published file if exists
        with open(publish_file_path, 'w') as f:
            f.write(script)

        self.logger.info(f'Published workflow: {self}')

        return self

    def run(self, **kwargs):
        c = Client(None)
        self.logger.info(f'Manual run workflow: {self}')
        c.trigger_dag(self.backend_id, **kwargs)

    def test(self, **kwargs):
        super(Workflow, self).test(**kwargs)  # noqa

    def backfill(self, **kwargs):
        super(Workflow, self).run(**kwargs)  # noqa


class PythonOperator(Stage, _PythonOperator):
    name_arg = 'task_id'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
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
        if self._script is None:
            fileloc = inspect.getfile(self.python_callable)
            with open(fileloc, 'r') as f:
                script = f.read()

            # Remove the __main__ guard
            script = re.sub(
                r'if\s+__name__\s+==\s+(?:"__main__"|\'__main__\'):(\n\s{4}.*)+', '', script
            )
            self._script = script.strip()
        return self._script


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
                self.logger.info(f'Triggering workflow {workflow_identifier}, Airflow dag_id={dag_id}')
                subprocess.run([sys.executable, '-m', 'airflow', 'dags', 'trigger', dag_id])
