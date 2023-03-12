#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Mar 05, 2023
"""
from shutil import copy2
from typing import TYPE_CHECKING

from dataci.db import db_connection

if TYPE_CHECKING:
    from .pipeline import Pipeline


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

    # Copy traced pipeline dvc.yaml
    copy2(pipeline.workdir / 'dvc.yaml', pipeline_workdir)

    #####################################################################
    # Step 3: Publish pipeline object to DB
    #####################################################################
    with db_connection:
        db_connection.execute("""
            INSERT INTO pipeline (name, version) VALUES (?,?)
            """, )
