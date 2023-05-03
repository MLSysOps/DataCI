#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanming.li@alibaba-inc.com
Date: May 03, 2023
"""
from functools import wraps
from typing import TYPE_CHECKING

from dataci.models import Workflow

if TYPE_CHECKING:
    from typing import Callable


def workflow(name=None, **kwargs) -> 'Callable[[Callable], Workflow]':
    """Workflow decorator. Convert the wrapped function into a
    :code:`dataci.pipeline.models.Workflow` object.
    """

    def decorator_workflow(run):
        @wraps(run)
        def wrapper_workflow():
            workflow_name = name or run.__name__
            # Convert stage name to camel case
            name_camel_case = ''.join(map(str.title, workflow_name.split('_'))) + 'Workflow'
            workflow_cls = type(
                name_camel_case, (Workflow,), kwargs,
            )
            # Initiate the models object with configured info (e.g., inputs, outputs, etc.)
            workflow_obj: Workflow = workflow_cls(name=workflow_name, **kwargs)
            # Build DAG from the wrapped `run` function
            with workflow_obj:
                run()

            return workflow_obj

        wrapped_func = getattr(wrapper_workflow, '__wrapped__')
        workflow_obj = wrapper_workflow()
        workflow_obj.__wrapped__ = wrapped_func
        return workflow_obj

    return decorator_workflow
