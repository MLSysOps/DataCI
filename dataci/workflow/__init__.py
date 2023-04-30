#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Feb 20, 2023
"""
import contextvars

from .decorator import stage
from .stage import Stage
from .workflow import Workflow

WORKFLOW_CONTEXT = contextvars.ContextVar('workflow_context', default={})

__all__ = ['Workflow', 'Stage', 'stage', 'WORKFLOW_CONTEXT']
