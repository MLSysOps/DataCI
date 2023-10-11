#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Oct 11, 2023
"""
from step01_unused_stage_w_util import outer_used_util
from step04_used_stage_w_util import outer_used_util as outer_used_util2
from step05_multi_file_stage_w_util.utils import some_util_function

def common_util():
    outer_used_util()
    outer_used_util2()
    some_util_function()
