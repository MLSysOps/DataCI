#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Mar 15, 2023
"""
from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from .run import Run


def save_run(run: 'Run'):
    #####################################################################
    # Step 1:
    # pipeline
    #####################################################################

    #####################################################################
    # Step 2: Track pipeline output feat by DVC
    #####################################################################
    # for output in run.pipeline.outputs:

    #####################################################################
    # Step 3: Publish run object to DB
    #####################################################################
    # TODO
    pass
