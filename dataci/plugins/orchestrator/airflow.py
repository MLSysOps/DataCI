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
from pathlib import Path
from typing import TYPE_CHECKING

import networkx as nx
from airflow.models import DAG as _DAG
from airflow.operators.python import PythonOperator as _PythonOperator

from dataci.models import Workflow, Stage, Dataset
from dataci.server.trigger import Trigger as _Trigger, EVENT_QUEUE, QUEUE_END

if TYPE_CHECKING:
    from typing import Any


class DAG(Workflow, _DAG):
    """A wrapper class for :code:`airflow.models.DAG`. Substituting the
    :code:`airflow.models.DAG` class with this class to provide version control
    over the DAGs.
    """
    BACKEND = 'airflow'

    name_arg = 'dag_id'

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
        if self._script is None:
            with open(self.fileloc, 'r') as f:
                script = f.read()
            # Remove stage import statements:
            # from xxx import stage_name / import stage_name
            stage_name_pattern = '|'.join([stage.name for stage in self.stages])
            import_pattern = re.compile(
                rf'^(?:from\s+[\w.]+\s+)?import\s+(?:{stage_name_pattern})[\r\t\f ]*\n', flags=re.MULTILINE
            )
            script = import_pattern.sub('', script)

            # Insert the stage scripts before the DAG script:
            for stage in self.stages:
                script = stage.script + '\n' * 2 + script

            self._script = script
        return self._script

    def publish(self):
        """Publish the DAG to the backend."""
        super().publish()
        # Copy the script content to the published file
        publish_file_path = (Path.home() / 'airflow' / 'dags' / self.name).with_suffix('.py')
        # Create parent dir if not exists
        publish_file_path.parent.mkdir(parents=True, exist_ok=True)
        # Remove the published file if exists
        with open(publish_file_path, 'w') as f:
            f.write(self.script)
        return self


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
            self._script = script
        return self._script

    def publish(self):
        super().publish()
        # Copy the script content to the published file
        publish_file_path = (Path.home() / 'airflow' / 'plugins' / self.name).with_suffix('.py')
        # Create parent dir if not exists
        publish_file_path.parent.mkdir(parents=True, exist_ok=True)
        # Remove the published file if exists
        with open(publish_file_path, 'w') as f:
            f.write(self.script)
        return self


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
                self.logger.info('Triggering workflow: %s', workflow_identifier)
                subprocess.run(['airflow', 'trigger_dag', workflow_identifier])
