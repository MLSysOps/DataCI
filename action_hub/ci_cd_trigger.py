#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanming.li@alibaba-inc.com
Date: May 08, 2023
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
    datasets = Dataset.find(dataset_name)
    # Get all stage versions to be substituted
    stages = Stage.find(stage_name)

    job_configs = list()
    # Build new workflow with substituted stages
    for w in workflows:
        for s in stages:
            for d in datasets:
                w.patch(s)
                w.cache()
                job_configs.append({
                    'workflow': w.identifier,
                    'dataset': d.identifier,
                })

    return job_configs


@stage()
def run(job_configs, **context):
    import re
    import traceback
    from dataci.models import Workflow

    def resolve_variable(var: str):
        # Locate variable
        if not isinstance(var, str):
            return var
        matched = re.findall(r'\$\{\{.*?}}', var)
        for replaceable in matched:
            match = replaceable.lstrip('${{').rstrip('}}').strip()
            # Resolve variable
            # TODO: Resolve variable
            if match == 'config.workflow':
                value = job_config['workflow']
            elif match == 'config.dataset':
                value = job_config['dataset']
            elif match == 'config.dataset.version':
                value = job_config['dataset'].split('@')[1]
            elif match == 'config.stage':
                value = job_config['stage']
            else:
                raise ValueError(f'Fail to parse variable: {replaceable}')
            var = var.replace(replaceable, value)
        return var

    ci_workflow_name = context['params']['ci_name']
    print(f'Built {len(job_configs)} jobs to run')
    num_success = 0
    for i, job_config in enumerate(job_configs):
        print(f'Running job {i + 1}/{len(job_configs)}...')
        job_success = False
        ci_workflow = ci_workflow_name + '@latest'
        try:
            ci_workflow = Workflow.get(ci_workflow_name + '@latest')
            print(f'Running workflow: {ci_workflow}')
            # Set workflow running parameters, resolve variables
            for k, v in ci_workflow.params.items():
                ci_workflow.params[k] = resolve_variable(v)
            # Set stage running parameters, resolve variables
            for stage in ci_workflow.stages:
                for k, v in stage.params.items():
                    stage.params[k] = resolve_variable(v)
            ci_workflow()
            job_success = True
            num_success += 1
        except Exception as e:
            print(f'CI/CD {ci_workflow} with config {job_config} failed with error: {e}')
            traceback.print_exc(chain=True)
        finally:
            print(f'Job {i + 1}/{len(job_configs)} {"succeed" if job_success else "failed"}')

    print(f'CI/CD workflow {ci_workflow_name} finished, {num_success}/{len(job_configs)} succeed.')


@workflow(
    name='official.ci_cd_trigger',
    params={
        'ci_name': None,
        'workflow': None,
        'stage': None,
        'dataset': None,
    }
)
def trigger_ci_cd():
    config_ci_runs >> run
