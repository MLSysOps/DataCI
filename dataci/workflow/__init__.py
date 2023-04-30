#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Feb 20, 2023
"""
import contextvars

from .decorator import stage
# from .pipeline import Pipeline
from .stage import Stage

WORKFLOW_CONTEXT = contextvars.ContextVar('workflow_context', default={})

__all__ = ['Pipeline', 'Stage', 'stage', 'WORKFLOW_CONTEXT']
