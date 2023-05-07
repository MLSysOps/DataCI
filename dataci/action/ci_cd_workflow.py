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
    workflow_identifier = context['params']['config']['workflow']
    dataset_identifier = context['params']['config']['dataset']

    workflow_obj = Workflow.get(workflow_identifier)
    # set workflow params
    workflow_obj.params['version'] = dataset_identifier.split('@')[1]
    workflow_obj()

    return workflow_obj.outputs


@stage()
def data_quality_check(**context):
    from dataci.models import Workflow

    workflow = Workflow.get('official/data_quality_check@1')
    workflow()

    return workflow.outputs


@stage()
def dc_bench(**context):
    pass


@stage()
def publish_train_dataset(**context):
    pass


@workflow(name='ci_cd_workflow')
def ci_cd_workflow():
    execute_workflow >> data_quality_check >> dc_bench >> publish_train_dataset
