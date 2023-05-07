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
    from dataci.models import Workflow, Dataset, Stage

    workflow_name = context['params']['workflow']
    stage_name = context['params']['stage']
    dataset_name = context['params']['dataset']
    # Get all workflow versions
    workflows = Workflow.find(workflow_name)
    # Get all dataset versions
    dataset = Dataset.find(dataset_name)
    # Get all stage versions to be substituted
    stages = Stage.find(stage_name)

    job_configs = list()
    # Build new workflow with substituted stages
    for w in workflows:
        for s in stages:
            w.patch(s)
            w.cache()
            job_configs.append({
                'workflow': w.identifier,
                'dataset': dataset.identifier,
            })

    return job_configs


@stage()
def run(job_configs, **context):
    from dataci.models import Workflow

    ci_workflow_name = context['params']['name']
    for job_config in job_configs:
        ci_workflow = Workflow.get(ci_workflow_name)
        # Set workflow running parameters
        ci_workflow.params['config'] = job_config
        ci_workflow()


@workflow(name='text_dataset_ci_config')
def trigger_ci_cd():
    config_ci_runs >> run
