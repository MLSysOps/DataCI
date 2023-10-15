#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Oct 11, 2023
"""
from dataci.plugins.decorators import stage


@stage
def unused_stage_w_util(df):
    # random data selection
    return df.sample(frac=1)


def outer_used_util():
    pass
