#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Oct 11, 2023
"""
from dataci.plugins.decorators import stage


@stage
def single_file_stage_v2(df):
    """A single file stage v2 to patch"""
    # random data selection
    return df.sample(frac=1)
