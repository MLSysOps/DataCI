#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: May 04, 2023
"""
from dataci.decorators.stage import stage
from dataci.decorators.workflow import workflow
from dataci.models import Stage


@stage()
def execute_workflow(**context):
    from dataci.models import Workflow

    workflow_identifier = context['params']['config']['workflow']
    dataset_identifier = context['params']['config']['dataset']

    workflow_obj = Workflow.get(workflow_identifier)
    # set workflow params
    workflow_obj.params['version'] = dataset_identifier.split('@')[1]
    outputs = workflow_obj()

    return outputs


data_quality_check = Stage.get('official.data_quality_check@1')

data_centric_benchmark = Stage.get('official.dc_bench@1')
data_centric_benchmark.params.update(
    {
        'type': 'data_aug',
        'ml_task': 'text_classification',
        'pass_when': 'metrics.test_acc > 0.9',
    }
)


@stage()
def publish_dataset(*inputs, **context):
    import subprocess

    subprocess.run(['dataci', 'publish', inputs[0]])


@workflow(name='ci_cd_workflow')
def ci_cd_workflow():
    execute_workflow >> data_quality_check >> data_centric_benchmark >> publish_dataset
