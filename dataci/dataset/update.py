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
    from dataci.dataset import Dataset
    from dataci.pipeline.pipeline import Pipeline

    #####################################################################
    # Step 1: Find changes made by parent dataset and yield pipeline
    #####################################################################
    logger.info('Searching changes...')
    update_plans = get_many_dataset_update_plan(dataset.name)
    datasets = set(map(lambda x: Dataset.from_dict({**x['parent_dataset'], 'repo': dataset.repo}), update_plans))
    pipelines = list(set(map(lambda x: Pipeline.from_dict({**x['pipeline'], 'repo': dataset.repo}), update_plans)))

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
    # Create unique dataset and pipeline
