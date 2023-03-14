#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Mar 05, 2023
"""
import logging
from shutil import copy2, copytree, rmtree
from typing import TYPE_CHECKING

from dataci.db.pipeline import create_one_pipeline

if TYPE_CHECKING:
    from .pipeline import Pipeline
    from .run import Run

logger = logging.getLogger(__name__)


def publish(pipeline: 'Pipeline' = ...):
    repo = pipeline.repo
    #####################################################################
    # Step 1: Build pipeline
    #####################################################################
    if not pipeline.is_built:
        pipeline.build()

    #####################################################################
    # Step 2: Create this pipeline at repo pipeline spec
    #####################################################################
    pipeline_dir = repo.pipeline_dir / pipeline.name
    pipeline_dir.mkdir(exist_ok=True)
    # Prepare directory at repo pipeline
    # repo/pipeline/<pipeline_name>/<version>
    pipeline_workdir = pipeline_dir / pipeline.version
    pipeline_workdir.mkdir(exist_ok=True)

    # repo/pipeline/<pipeline_name>/latest/runs
    pipeline_run_dir = pipeline_workdir / 'runs'
    pipeline_run_dir.mkdir(exist_ok=True)

    # Prepare common feat
    # repo/pipeline/<pipeline_name>/feat
    pipeline_feat_dir = pipeline_workdir / pipeline.FEAT_DIR
    copytree(pipeline.workdir / pipeline.FEAT_DIR, pipeline_feat_dir, symlinks=True)

    # Copy traced pipeline dvc.yaml
    copy2(pipeline.workdir / 'dvc.yaml', pipeline_workdir)

    # Copy pipeline code
    if (pipeline_workdir / pipeline.CODE_DIR).exists():
        rmtree(pipeline_workdir / pipeline.CODE_DIR)
    copytree(pipeline.workdir / pipeline.CODE_DIR, pipeline_workdir / pipeline.CODE_DIR)

    #####################################################################
    # Step 3: Publish pipeline object to DB
    #####################################################################
    create_one_pipeline(pipeline.dict())


def save_run(run: 'Run'):
    #####################################################################
    # Step 1:
    # pipeline
    #####################################################################

    #####################################################################
    # Step 2: Track pipeline output feat by DVC
    #####################################################################

    #####################################################################
    # Step 3: Publish run object to DB
    #####################################################################
    # TODO
    pass
