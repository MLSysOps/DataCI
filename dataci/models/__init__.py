#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Feb 20, 2023
"""
import contextvars

WORKFLOW_CONTEXT = contextvars.ContextVar(
    'workflow_context',
    default={'params': dict()}
)

from .workspace import Workspace
from .stage import Stage
from .workflow import Workflow
from .dataset import Dataset

__all__ = [
    'Workspace', 'Dataset', 'Workflow', 'Stage', 'WORKFLOW_CONTEXT'
]
