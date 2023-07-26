#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Jun 11, 2023
"""
import functools
import inspect
import re
import sys
from pathlib import Path
from typing import TYPE_CHECKING

import networkx as nx
from airflow.models import DAG as _DAG
from airflow.operators.python import PythonOperator as _PythonOperator
from airflow.utils.decorators import fixup_decorator_warning_stack

from dataci.models import Workflow, Stage, Dataset

if TYPE_CHECKING:
    from typing import Callable, Any


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
    def script(self):
        if self._script is None:
            with open(self.fileloc, 'r') as f:
                script = f.read()
            # Remove stage import statements:
            # from xxx import stage_name / import stage_name
            stage_name_pattern = '|'.join([stage.name for stage in self.stages])
            import_pattern = re.compile(
                rf'^(?:from\s+[\w\.]+\s+)?import\s+(?:{stage_name_pattern})[\r\t\f ]*\n', flags=re.MULTILINE
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


def dag(
        dag_id: 'str' = "", **kwargs,
) -> 'Callable[[Callable], Callable[..., DAG]]':
    """
    Copy from Python dag decorator, replacing :code:`airflow.models.dag.DAG` to
    :code:`dataci.orchestrator.airflow.DAG`. Wraps a function into an Airflow DAG.
    Accepts kwargs for operator kwarg. Can be used to parameterize DAGs.

    :param dag_args: Arguments for DAG object
    :param dag_kwargs: Kwargs for DAG object.
    """

    def wrapper(f: 'Callable') -> 'Callable[..., DAG]':
        @functools.wraps(f)
        def factory(*factory_args, **factory_kwargs):
            # Generate signature for decorated function and bind the arguments when called
            # we do this to extract parameters, so we can annotate them on the DAG object.
            # In addition, this fails if we are missing any args/kwargs with TypeError as expected.
            f_sig = inspect.signature(f).bind(*factory_args, **factory_kwargs)
            # Apply defaults to capture default values if set.
            f_sig.apply_defaults()

            # Initialize DAG with bound arguments
            with DAG(
                    dag_id or f.__name__,
                    **kwargs,
            ) as dag_obj:
                # Set DAG documentation from function documentation if it exists and doc_md is not set.
                if f.__doc__ and not dag_obj.doc_md:
                    dag_obj.doc_md = f.__doc__

                # Generate DAGParam for each function arg/kwarg and replace it for calling the function.
                # All args/kwargs for function will be DAGParam object and replaced on execution time.
                f_kwargs = {}
                for name, value in f_sig.arguments.items():
                    f_kwargs[name] = dag_obj.param(name, value)

                # set file location to caller source path
                back = sys._getframe().f_back
                dag_obj.fileloc = back.f_code.co_filename if back else ""

                # Invoke function to create operators in the DAG scope.
                f(**f_kwargs)

            # Return dag object such that it's accessible in Globals.
            return dag_obj

        # Ensure that warnings from inside DAG() are emitted from the caller, not here
        fixup_decorator_warning_stack(factory)
        return factory

    return wrapper


class PythonOperator(Stage, _PythonOperator):
    name_arg = 'task_id'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__init_args = (*args, *kwargs.items())

    def execute_callable(self) -> 'Any':
        """Execute the python callable."""
        # Resolve input table
        bound = inspect.signature(self.python_callable).bind(*self.op_args, **self.op_kwargs)
        for arg_name, _ in self.input_table.items():
            if arg_name in bound.arguments:
                dataset = Dataset.get(bound.arguments[arg_name])
                bound.arguments[arg_name] = dataset.dataset_files.read()
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
                ret[key] = dataset.identifier
            else:
                dataset.dataset_files = ret
                dataset.save()
                ret = dataset.identifier
            self.log.info(f'Output table {key}: {dataset.identifier}')
        return ret

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
