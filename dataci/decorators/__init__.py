#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanming.li@alibaba-inc.com
Date: May 03, 2023
"""
from typing import TYPE_CHECKING

from dataci.decorators.airflow import python_task, Dataset
from dataci.orchestrator.airflow import dag

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
