#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Mar 16, 2023
"""
from dataci.run.list import list_run


def test_get_runs():
    runs = list_run('build_text_cls_train_data')
    print(runs)


if __name__ == '__main__':
    test_get_runs()
