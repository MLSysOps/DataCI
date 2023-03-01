#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Mar 1, 2023
"""
from functools import partial
from typing import Type

from . import Stage


def stage(name, inputs, outputs, dependecy='auto', repo=None):
    def decorator_stage(run):
        def wrapper_stage():
            # Convert stage name to camel case
            name_components = name.split('_')
            name_camel_case = name_components[0] + ''.join(x.title() for x in name_components[1:]) + 'Stage'
            # Generate stage class of the wrapped `run` function
            stage_cls: 'Type[Stage]' = type(
                name_camel_case, (Stage,), {'run': lambda self, *args, **kwargs: run(*args, **kwargs)},
            )
            # Initiate the stage object with configured info (e.g., inputs, outputs, etc.)
            stage_obj = stage_cls(
                name=name, inputs=inputs, outputs=outputs, dependency=dependecy, repo=repo
            )
            return stage_obj
        return wrapper_stage()
    return decorator_stage
