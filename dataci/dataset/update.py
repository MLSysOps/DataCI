#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Mar 16, 2023
"""
import logging
from typing import TYPE_CHECKING

from dataci.db.pipeline import get_many_pipeline

if TYPE_CHECKING:
    from dataci.dataset import Dataset

logger = logging.getLogger(__name__)


def update_dataset(dataset: 'Dataset' = ...):
    #####################################################################
    # Step 1: Find changes made by parent dataset and yield pipeline
    #####################################################################
    logger.info('Searching changes...')
    # pipeline
    pipelines = get_many_pipeline(dataset.yield_pipeline.name, '*')
    # parent dataset
    datasets = ...
    # pipeline x parent dataset - run.pipeline x run.dataset

    #####################################################################
    # Step 2: Execute changes
    #####################################################################
