#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Mar 1, 2023
"""
from typing import Callable

from .stage import Stage


def stage(inputs, outputs, name=None, dependency='auto', repo=None) -> Callable[[Callable], Stage]:
    """Pipeline stage decorator. Convert the wrapped function into a
    :code:`dataci.pipeline.stage.Stage` object.

    Args:
        inputs (str or os.PathLike): Stage inputs dataset name or data path.
        outputs (str os os.PathLike): Stage outputs dataset name or data path.
        name (str): Stage name. Default to `None`, which will infer from the name of wrapped function.
        dependency (str or list of str): Dependencies of the stage. Default to 'auto', which will
            automatically applied all the stage inputs and the stage's code.
        repo (dataci.repo.Repo): Dataci Repo. Default to `None`, which will automatically infer the
            repo location.
    """

    def decorator_stage(run):
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
            stage_obj: Stage = stage_cls(
                name=name, inputs=inputs, outputs=outputs, dependency=dependency, repo=repo
            )
            return stage_obj
        return wrapper_stage()
    return decorator_stage
