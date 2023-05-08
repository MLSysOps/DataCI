#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanming.li@alibaba-inc.com
Date: May 08, 2023
"""
from dataci.decorators.stage import stage


@stage(name='official.dc_bench')
def data_centric_benchmark(dataset_identifier, **context):
    print('Dummy dc benchmark')
    return dataset_identifier


if __name__ == '__main__':
    data_centric_benchmark.publish()
