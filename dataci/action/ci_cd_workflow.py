#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: May 04, 2023
"""
from dataci.decorators.stage import stage
from dataci.decorators.workflow import workflow, Workflow


@stage()
def execute_workflow(**context):
    workflow_identifier = context['params']['workflow']
    workflow_obj = Workflow.get(workflow_identifier)
    # Reset workflow params
    workflow_obj.params['input_version'] = context['params']['input_version']
    workflow_obj.params['output_version'] = context['params']['output_version']
    workflow_obj()


@stage()
def data_quality_check(**context):
    pass


@stage()
def dc_bench(**context):
    pass


@stage()
def publish_train_dataset(**context):
    pass


@workflow(name='ci_cd_workflow')
def ci_cd_workflow():
    execute_workflow >> data_quality_check >> dc_bench >> publish_train_dataset
