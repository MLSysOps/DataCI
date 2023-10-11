#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Oct 11, 2023
"""
from dataci.plugins.decorators import stage


@stage
def multi_file_stage(df):
    # random data selection
    return df.sample(frac=1)
