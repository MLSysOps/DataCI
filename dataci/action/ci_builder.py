#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: May 05, 2023
"""
import logging
import re

from dataci.models import Stage, Workflow
from dataci.opteraotrs.execute_workflow_operator import ExecuteWorkflowOperator
from dataci.opteraotrs.shell_command_operator import ShellCommandOperator

logger = logging.getLogger(__name__)


def build_ci_workflow(config: dict):
    build_steps = list()
    for step_config in config['jobs']['steps']:
        step_name = step_config['name'].replace(' ', '_')
        if step_config.get('uses', None):
            if matched := re.match(r'(\w+)/(\w+)/(\w+)(?:@([\da-f]+))?',
                                   step_config['uses']):  # uses is a fixed resources
                workspace_name, id_type, name, version = matched.groups()
                # If id_type is workflows:
                if id_type == 'workflows':
                    workflow_identifier = f'{workspace_name}.{name}@{version}'
                    step = ExecuteWorkflowOperator(name=step_name, workflow_identifier=workflow_identifier)
                    # Update parameters
                    step.params.update(step_config.get('with', dict()))
                elif id_type == 'stages':
                    step = Stage.get(f'{workspace_name}.{name}@{version}')
                    # Update parameters
                    step.params.update(step_config.get('with', dict()))
                else:
                    raise ValueError(f'Invalid step config: {step_config}, error at uses: {step_config["uses"]}')
            elif (matched := re.match(
                    r'\$\{\{\s*([\w.]+)\s*}}', step_config['uses'], re.I | re.M)
            ) is not None:  # uses is a variable
                var_name = matched.group(1)
                if var_name == 'config.workflow':
                    step = ExecuteWorkflowOperator(name=step_name, workflow_identifier=var_name)
                    # Update parameters
                    step.params.update(step_config.get('with', dict()))
                else:
                    raise ValueError(f'Invalid step config: {step_config}, error at uses: {step_config["uses"]}')
            else:
                raise ValueError(f'Invalid step config: {step_config}, error at uses: {step_config["uses"]}')
        # If step is a run step
        elif step_config.get('run', None):
            step = ShellCommandOperator(name=step_name, command=step_config['run'])
        else:
            raise ValueError(f'Invalid step config: {step_config}')

        build_steps.append(step)

    # Build workflow
    workflow = Workflow(
        name=config['name'],
        params=config['config'],
    )
    with workflow:
        step0 = build_steps.pop(0)
        for step in build_steps:
            step0 = step0 >> step
    workflow.publish()

    return workflow


def build_ci_trigger_workflow(ci_config: dict):
    # Obtain CI config workflow template
    workflow_template = Workflow.get('official.ci_cd_trigger@latest')

    # Config workflow name
    workflow_template.name = ci_config['name'] + '_trigger'

    # Config workflow schedule (triggered events)
    for event, producer in (ci_config['on'] or dict()).items():
        # Schedule is a property, need to get and set
        workflow_template.schedule += [f'@event {producer} {event} success']

    # Config params
    workflow_template.params = ci_config['config']

    # Publish workflow
    workflow_template.publish()

    return workflow_template


def build(ci_config: dict):
    logger.info('Building CI/CD job workflow:')
    print(build_ci_workflow(ci_config))
    logger.info('Building CI/CD trigger workflow: ')
    print(build_ci_trigger_workflow(ci_config))
