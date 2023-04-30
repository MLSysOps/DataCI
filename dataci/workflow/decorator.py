#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Mar 1, 2023
"""
from functools import wraps
from typing import Callable

from .stage import Stage


def stage(name=None) -> Callable[[Callable], Stage]:
    """Pipeline stage decorator. Convert the wrapped function into a
    :code:`dataci.pipeline.stage.Stage` object.
    """

    def decorator_stage(run):
        @wraps(run)
        def wrapper_stage():
            stage_name = name or run.__name__
            # Convert stage name to camel case
            name_components = stage_name.split('_')
            name_camel_case = name_components[0] + ''.join(x.title() for x in name_components[1:]) + 'Stage'
            # Generate stage class of the wrapped `run` function
            stage_cls = type(
                name_camel_case, (Stage,), {'run': lambda self, *args, **kwargs: run(*args, **kwargs)},
            )
            # Initiate the stage object with configured info (e.g., inputs, outputs, etc.)
            stage_obj: Stage = stage_cls(name=stage_name)
            return stage_obj

        wrapped_func = getattr(wrapper_stage, '__wrapped__')
        stage_obj = wrapper_stage()
        stage_obj.__wrapped__ = wrapped_func
        return stage_obj
    return decorator_stage
