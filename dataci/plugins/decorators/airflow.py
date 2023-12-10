#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Jun 12, 2023
"""
import functools
import inspect
import sys
from typing import TYPE_CHECKING, cast

import attr
from airflow.decorators.base import TaskDecorator as AirflowTaskDecorator, _TaskDecorator as _AirflowTaskDecorator
from airflow.decorators.python import _PythonDecoratedOperator
from airflow.models.xcom_arg import PlainXComArg
from airflow.utils.decorators import fixup_decorator_warning_stack

from dataci.decorators.base import DecoratedOperatorStageMixin
from dataci.models import Stage, Dataset as _Dataset
from ..orchestrator.airflow import PythonOperator, DAG
from dataci.config import DISABLE_WORKFLOW_BUILD

if TYPE_CHECKING:
    from typing import Callable

    from airflow.models import BaseOperator


class _TaskDecorator(_AirflowTaskDecorator, DecoratedOperatorStageMixin):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Make dummy op_args, op_kwargs according to the signature of the decorated function (self.function)
        # This is to fetch the desired stage object initialized in the __call__ method
        # and pass it the outer TaskDecorator object
        sig = inspect.signature(self.function)
        op_args, op_kwargs = tuple(), dict()
        for name, param in sig.parameters.items():
            if param.kind in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD):
                op_args += (param.default,)
            elif param.kind == inspect.Parameter.KEYWORD_ONLY:
                op_kwargs.update({name: param.default})
        op = self.operator_class(
            python_callable=self.function,
            op_args=op_args,
            op_kwargs=op_kwargs,
            multiple_outputs=self.multiple_outputs,
            **self.kwargs,
        )
        self._stage = cast(Stage, op)

    def __call__(self, *args, **kwargs):
        # Check if the input arguments are dataset
        # If so, mark the argument as input table
        bound = inspect.signature(self.function).bind(*args, **kwargs)
        for key, arg in bound.arguments.items():
            if isinstance(getattr(arg, 'operator', None), Stage):  # arg.operator is DataCI stage
                if arg.operator.output_table:  # arg.operator's output is a dataset
                    # Mark the dataset as input table, provide a dummy value
                    self._stage.input_table[key] = ...
            elif isinstance(arg, _Dataset):  # arg is a DataCI dataset
                self._stage.input_table[key] = {
                    'name': arg.identifier, 'file_reader': arg.file_reader.NAME, 'file_writer': arg.file_writer.NAME,
                    'type': arg.type_name,
                }
                # Rewrite the argument with the dataset identifier
                bound.arguments[key] = arg.identifier

        xcom_arg = super().__call__(*bound.args, **bound.kwargs)
        # Replace the _stage attribute with the newly created operator object
        op = xcom_arg.operator
        if isinstance(op, Stage):
            input_table, output_table = self._stage.input_table, self._stage.output_table
            self._stage = cast(Stage, op)
            # TODO: use reload to update the stage object
            self._stage.input_table, self._stage.output_table = input_table, output_table
        return xcom_arg


class TaskDecorator(Stage, AirflowTaskDecorator):
    pass


def task_decorator_factory(
        python_callable: 'Callable | None' = None,
        *,
        multiple_outputs: 'bool | None' = None,
        decorated_operator_class: 'type[BaseOperator]',
        **kwargs,
) -> 'TaskDecorator':
    """Copy from airflow source code, and modify the decorator class to version controlled
    :code:`TaskDecorator`.
    Generate a wrapper that wraps a function into an Airflow operator.

    Can be reused in a single DAG.

    :param python_callable: Function to decorate.
    :param multiple_outputs: If set to True, the decorated function's return
        value will be unrolled to multiple XCom values. Dict will unroll to XCom
        values with its keys as XCom keys. If set to False (default), only at
        most one XCom value is pushed.
    :param decorated_operator_class: The operator that executes the logic needed
        to run the python function in the correct environment.

    Other kwargs are directly forwarded to the underlying operator class when
    it's instantiated.
    """
    if multiple_outputs is None:
        multiple_outputs = cast(bool, attr.NOTHING)
    if python_callable:
        decorator = _TaskDecorator(
            function=python_callable,
            multiple_outputs=multiple_outputs,
            operator_class=decorated_operator_class,
            kwargs=kwargs,
        )
        return cast(TaskDecorator, decorator)
    elif python_callable is not None:
        raise TypeError("No args allowed while using @task, use kwargs instead")

    def decorator_factory(python_callable):
        dec = _TaskDecorator(
            function=python_callable,
            multiple_outputs=multiple_outputs,
            operator_class=decorated_operator_class,
            kwargs=kwargs,
        )
        return dec

    return cast(TaskDecorator, decorator_factory)


class PythonDecoratedOperator(_PythonDecoratedOperator, PythonOperator):
    """
    Wraps a Python callable and captures args/kwargs when called for execution.

    :param python_callable: A reference to an object that is callable
    :param op_kwargs: a dictionary of keyword arguments that will get unpacked
        in your function (templated)
    :param op_args: a list of positional arguments that will get unpacked when
        calling your callable (templated)
    :param multiple_outputs: If set to True, the decorated function's return value will be unrolled to
        multiple XCom values. Dict will unroll to XCom values with its keys as XCom keys. Defaults to False.
    """


def python_task(
        python_callable: 'Callable | None' = None,
        multiple_outputs: 'bool | None' = None,
        **kwargs,
) -> 'TaskDecorator':
    """Wraps a function into an Airflow operator.

    Accepts kwargs for operator kwarg. Can be reused in a single DAG.

    :param python_callable: Function to decorate
    :param multiple_outputs: If set to True, the decorated function's return value will be unrolled to
        multiple XCom values. Dict will unroll to XCom values with its keys as XCom keys. Defaults to False.
    """
    return task_decorator_factory(
        python_callable=python_callable,
        multiple_outputs=multiple_outputs,
        decorated_operator_class=PythonDecoratedOperator,
        **kwargs,
    )


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

                # disable dag building when stage is building
                # this is to prevent recursive call dag building when written in the same script as stage
                if DISABLE_WORKFLOW_BUILD.is_set():
                    dag_obj.log.warning(
                        "DAG building is disabled when stage is building. To disable this warning, "
                        "write dag definition in a separate file."
                    )
                    return dag_obj

                # Invoke function to create operators in the DAG scope.
                f(**f_kwargs)

            # Return dag object such that it's accessible in Globals.
            return dag_obj

        # Ensure that warnings from inside DAG() are emitted from the caller, not here
        fixup_decorator_warning_stack(factory)
        return factory

    return wrapper


class Dataset:
    def __new__(cls, name: str, dataset_files: PlainXComArg = None, file_reader='auto', file_writer='csv', **kwargs):
        # parse multiple outputs from XComArg
        if dataset_files is not None:
            if isinstance(dataset_files, PlainXComArg):
                dataset = _Dataset(name=name, file_reader=file_reader, file_writer=file_writer, **kwargs)
                dataset_files.operator.output_table[dataset_files.key] = dataset

        return dataset_files

    @classmethod
    def get(cls, name: str, version: str = None, file_reader='auto', file_writer='csv'):
        # Dummy dataset get, create a dataset object with name and version
        workspace, name, version = _Dataset.parse_data_model_get_identifier(name, version)
        dataset = _Dataset(f'{workspace}.{name}', file_reader=file_reader, file_writer=file_writer)
        dataset.version = version
        return dataset
