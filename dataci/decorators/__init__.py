#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanming.li@alibaba-inc.com
Date: May 03, 2023
"""
from typing import TYPE_CHECKING

from dataci.decorators.stage import python_stage

if TYPE_CHECKING:
    from typing import Any


class StageDecoratorCollection:
    """Implementation to provide the ``@stage`` syntax."""

    python = staticmethod(python_stage)

    __call__: 'Any' = python  # Alias '@stage' to '@stage.python'.


stage = StageDecoratorCollection()
