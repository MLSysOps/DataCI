#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Mar 05, 2023
"""
import os
from distutils.dir_util import copy_tree
from shutil import copy2

from dataci.repo import Repo
from .pipeline import Pipeline


def publish_pipeline(repo: 'Repo', pipeline: 'Pipeline'):
    # Locate pipeline build directory
    print(pipeline.workdir)

    # Locate repo pipeline directory
    print(repo.pipeline_dir)

    # Create this pipeline at repo
    pipeline_dir = repo.pipeline_dir / pipeline.name
    pipeline_dir.mkdir(exist_ok=True)
    # Prepare directory at repo pipeline
    # repo/pipeline/<pipeline_name>/<version>
    pipeline_workdir = pipeline_dir / pipeline.version
    pipeline_workdir.mkdir(exist_ok=True)
    # repo/pipeline/<pipeline_name>/latest -> repo/pipeline/<pipeline_name>/<version>
    pipeline_latest_dir = pipeline_dir / 'latest'
    try:
        pipeline_latest_dir.unlink()
    except FileNotFoundError:
        try:
            os.remove(pipeline_latest_dir)
        except FileNotFoundError:
            pass
    pipeline_latest_dir.symlink_to(pipeline_workdir, target_is_directory=True)
    # repo/pipeline/<pipeline_name>/latest/runs
    pipeline_run_dir = pipeline_workdir / 'runs'
    pipeline_run_dir.mkdir(exist_ok=True)
    # Get the newest run number
    max_run = max([f for f in pipeline_run_dir.glob('*') if f.is_dir()], default=0) + 1
    # repo/pipeline/<pipeline_name>/latest/runs/<max_run>
    pipeline_cur_run_dir = pipeline_run_dir / str(max_run)
    pipeline_cur_run_dir.mkdir(exist_ok=True)
    # repo/pipeline/<pipeline_name>/latest/feat -> repo/pipeline/<pipeline_name>/latest/runs/<max_run>
    pipeline_latest_run_dir = pipeline_workdir / pipeline.FEAT_DIR
    try:
        pipeline_latest_run_dir.unlink()
    except FileNotFoundError:
        try:
            os.remove(pipeline_latest_run_dir)
        except FileNotFoundError:
            pass
    pipeline_latest_run_dir.symlink_to(pipeline_cur_run_dir, target_is_directory=True)

    # Copy code, feat -> pipeline/<pipeline_name>/latest/
    copy_tree(str(pipeline.workdir / pipeline.CODE_DIR), str(pipeline_workdir / pipeline.CODE_DIR),
              preserve_symlinks=True)
    copy_tree(str(pipeline.workdir / pipeline.FEAT_DIR), str(pipeline_workdir / pipeline.FEAT_DIR),
              preserve_symlinks=True)
    # Copy traced pipeline dvc.yaml
    copy2(pipeline.workdir / 'dvc.yaml', pipeline_workdir)
    # Copy pipeline run result to running result dir (if any)
    run_lock_file = pipeline.workdir / 'dvc.lock'
    if run_lock_file.exists():
        copy2(pipeline.workdir / 'dvc.lock', pipeline_cur_run_dir)
    # Link pipeline run result dvc.lock to pipeline latest dir
    run_result_tracker = pipeline_workdir / 'dvc.lock'
    try:
        run_result_tracker.unlink()
    except FileNotFoundError:
        try:
            os.remove(run_result_tracker)
        except FileNotFoundError:
            pass
    run_result_tracker.symlink_to(pipeline_workdir / pipeline.FEAT_DIR / 'dvc.lock')

    # Update current pipeline to parent dataset
    # TODO
