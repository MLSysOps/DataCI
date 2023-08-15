#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Aug 16, 2023
"""
from typing import TYPE_CHECKING

from .airflow import python_task, dag, Dataset

if TYPE_CHECKING:
    from typing import Any

__all__ = [
    'StageDecoratorCollection',
    'dag',
    'stage',
    'Dataset',
]


class StageDecoratorCollection:
    """Implementation to provide the ``@stage`` syntax."""

    python = staticmethod(python_task)

    __call__: 'Any' = python  # Alias '@stage' to '@stage.python'.


stage = StageDecoratorCollection()
