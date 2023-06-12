#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Jun 12, 2023
"""
import inspect
from textwrap import dedent
from typing import TYPE_CHECKING

from airflow.decorators.base import task_decorator_factory
from airflow.decorators.python import _PythonDecoratedOperator

from dataci.orchestrator.airflow import PythonOperator

if TYPE_CHECKING:
    from airflow.decorators import TaskDecorator


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

    @property
    def script(self):
        return dedent(inspect.getsource(self.python_callable))


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
