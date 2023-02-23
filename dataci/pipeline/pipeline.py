#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: FebK 23, 2023
"""
import os
from pathlib import Path

from dataci.repo import Repo


class Pipeline(object):
    def __init__(self, repo: Repo, name, version=None, basedir=os.curdir):
        self.repo = repo
        self.name = name
        self.version = version or 'latest'
        self.basedir = Path(basedir)

        # prepare working directory
        self.workdir = self.basedir / self.name / self.version
        self.code_dir = self.workdir / 'code'
        self.feat_dir = self.workdir / 'feat'
        self.workdir.mkdir(exist_ok=True, parents=True)
        self.code_dir.mkdir(exist_ok=True)
        self.feat_dir.mkdir(exist_ok=True)

        # stages
        self.stages = list()

    def add_stage(self, stage):
        self.stages.append(stage)

    def build(self):
        for stage in self.stages:
            # For each stage
            # resolve input and output feature path

            # pack the code and all its dependencies to `code_dir`

            # manage stages by dvc
            # dvc stage add -n <stage name> -d xxx.py -d input.csv -O xxx.csv -w self.workdir python.py
            cmd = ['dvc', 'stage', 'add', '-n', str(stage.name), '-d', str(stage.dependency), '-O', stage.outputs, '-w',
                   str(self.workdir), 'python', f'{stage.name}.py']

    def __call__(self):
        # dvc repo
        pass
