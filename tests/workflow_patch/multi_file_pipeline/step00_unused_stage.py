#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Oct 11, 2023
"""
from dataci.plugins.decorators import stage
from step02_with_util_stage import in_stage_util_function

@stage
def unused_stage(df):
    in_stage_util_function()
    # random data selection
    return df.sample(frac=1)