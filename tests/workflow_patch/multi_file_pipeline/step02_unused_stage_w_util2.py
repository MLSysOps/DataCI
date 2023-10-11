#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Oct 11, 2023
"""
from dataci.plugins.decorators import stage

import augly.text as textaugs


def outer_used_util2():
    # use inner stage
    print(unused_stage_w_util2)


@stage
def unused_stage_w_util2(df):
    # random data selection
    return df.sample(frac=1)