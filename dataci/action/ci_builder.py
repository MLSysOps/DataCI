#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: May 05, 2023
"""
from dataci.models.workflow import Workflow


def build_ci_workflow(job_config: dict):
    pass


def build_ci_trigger_workflow(ci_config: dict):
    # Obtain CI config workflow template
    workflow_template = Workflow.get('official.ci_cd_trigger@latest')

    # Config workflow name
    workflow_template.name = ci_config['name'] + '_trigger'

    # Config workflow schedule (triggered events)
    for event, producer in (ci_config['on'] or dict()).items():
        workflow_template.schedule.append(f'@event {producer} {event} success')

    # Config params
    workflow_template.params['config'] = ci_config['config']

