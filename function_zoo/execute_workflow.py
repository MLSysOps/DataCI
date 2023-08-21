#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: May 16, 2023
"""
from dataci.decorators.stage import stage


@stage(name='official.execute_workflow')
def execute_workflow(**context):
    workflow = context['params']['workflow']
    print(f'Execute workflow: {workflow}')
    return workflow()
