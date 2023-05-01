#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Feb 20, 2023
"""
import contextvars

WORKFLOW_CONTEXT = contextvars.ContextVar('workflow_context', default={})

from .stage import Stage
from .workflow import Workflow
from .decorator import stage, workflow

__all__ = [
    'Workflow', 'workflow', 'Stage', 'stage', 'WORKFLOW_CONTEXT'
]
