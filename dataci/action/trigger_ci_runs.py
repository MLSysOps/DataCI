#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: May 05, 2023
"""
from dataci.decorators.stage import stage
from dataci.decorators.workflow import workflow


@stage()
def config_ci_runs(**context):
    pass


@stage()
def run(**context):
    pass


@workflow(name='config_ci_cd_workflow')
def trigger_ci_cd():
    config_ci_runs >> run
