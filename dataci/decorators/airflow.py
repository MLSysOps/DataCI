#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Jun 12, 2023
"""
from typing import TYPE_CHECKING, cast

import attr
from airflow.decorators.base import TaskDecorator as AirflowTaskDecorator, _TaskDecorator as _AirflowTaskDecorator
from airflow.decorators.python import _PythonDecoratedOperator

from dataci.models import Stage
from dataci.orchestrator.airflow import PythonOperator

if TYPE_CHECKING:
    from airflow.models import BaseOperator


class _TaskDecorator(_AirflowTaskDecorator, Stage):
    name_arg = 'task_id'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        Stage.__init__(self, *args, **self.kwargs)

    def __call__(self, *args, **kwargs):
        xcom_arg = super().__call__(*args, **kwargs)
        # Copy the current stage related info to the xcom arg operator
        op = xcom_arg.operator
        op.workspace = self.workspace
        op.name = self.name
        op.version = self.version
        op.create_date = self.create_date
        op._script = self._script
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
        print(dec.name)
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
