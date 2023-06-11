#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Mar 1, 2023
"""
from functools import wraps
from typing import TYPE_CHECKING

from airflow.decorators import task

if TYPE_CHECKING:
    from typing import Callable
    from dataci.models import Stage


def python_stage(*args, **kwargs) -> 'Callable[[Callable], Stage]':
    """Pipeline stage decorator. Convert the wrapped function into a
    :code:`dataci.pipeline.stage.Stage` object.
    """

    def decorator_stage(run):
        from dataci.models import Stage

        @wraps(run)
        def wrapper_stage():
            # Decorate run with airflow task decorator
            operator_class = task(**kwargs)(run)

            # Initiate the stage object with configured info (e.g., inputs, outputs, etc.)
            return Stage(operator_class)

        wrapped_func = getattr(wrapper_stage, '__wrapped__')
        stage_obj = wrapper_stage()
        stage_obj.__wrapped__ = wrapped_func
        return stage_obj

    if len(args) == 1 and callable(args[0]) and not kwargs:
        return decorator_stage(args[0])

    return decorator_stage
