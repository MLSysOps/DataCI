#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Mar 16, 2023
"""
import logging
from typing import TYPE_CHECKING

from dataci.db.dataset import get_many_dataset_update_plan

if TYPE_CHECKING:
    from dataci.dataset import Dataset

logger = logging.getLogger(__name__)


def update(dataset: 'Dataset' = ...):
    from dataci.pipeline.pipeline import Pipeline

    #####################################################################
    # Step 1: Find changes made by parent dataset and yield pipeline
    #####################################################################
    logger.info('Searching changes...')
    update_plans = get_many_dataset_update_plan(dataset.name)
    datasets = set(map(lambda x: (x['parent_dataset']['name'], x['parent_dataset']['version']), update_plans))
    pipelines = set(map(lambda x: (x['pipeline']['name'], x['pipeline']['version']), update_plans))

    logger.info(f'Found {len(update_plans)} possible updates:')
    if len(update_plans) > 0:
        logger.info(f'| S.N. | {"Parent dataset":30} >>> {"Pipeline":30} |')
    for i, plan in enumerate(update_plans, 1):
        dataset_dict, pipeline_dict = plan['parent_dataset'], plan['pipeline']
        logger.info(
            f'| {i:4d} | {dataset_dict["name"] + "@" + dataset_dict["version"][:7]:30} '
            f'>>> {pipeline_dict["name"] + "@" + pipeline_dict["version"][:7]:30} |'
        )
    logger.info(f'Total {len(datasets)} dataset version(s), {len(pipelines)} pipeline versions(s).')

    #####################################################################
    # Step 2: Execute plans
    #####################################################################
    logger.info('Executing dataset update...')
    for i, plan in enumerate(update_plans, 1):
        dataset_dict, pipeline_dict = plan['parent_dataset'], plan['pipeline']
        pipeline = Pipeline.from_dict({**pipeline_dict, 'repo': dataset.repo})
        # replace pipeline input dataset
        for stage in pipeline.stages:
            if stage._inputs.split('@')[0] == dataset_dict['name']:
                stage._inputs = f'{dataset_dict["name"]}@{dataset_dict["version"]}'
        # Run pipeline
        pipeline()
        logger.info(f'Finish {i}/{len(update_plans)}')
