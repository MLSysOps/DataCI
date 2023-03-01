#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Feb 23, 2023
"""
import os
import subprocess
from pathlib import Path
from typing import Iterable, Union

from dataci.repo import Repo
from .stage import Stage
from .utils import cwd


class Pipeline(object):
    CODE_DIR = 'code'
    FEAT_DIR = 'feat'

    def __init__(
            self,
            name: str,
            version: str = None,
            basedir: os.PathLike = os.curdir,
            repo: Repo = None,
            stages: Union[Iterable[Stage], Stage] = None
    ):
        self.repo = repo or Repo()
        self.name = name
        self.version = version or 'latest'
        self.basedir = Path(basedir)

        # prepare working directory
        self.workdir = (self.basedir / self.name / self.version).resolve()
        self.workdir.mkdir(exist_ok=True, parents=True)
        (self.workdir / self.CODE_DIR).mkdir(exist_ok=True)
        (self.workdir / self.FEAT_DIR).mkdir(exist_ok=True)

        # stages
        self.stages = stages or list()
        if not isinstance(self.stages, Iterable):
            self.stages = [self.stages]

    def add_stage(self, stage: Stage):
        self.stages.append(stage)

    def build(self):
        with cwd(self.workdir):
            for stage in self.stages:
                # For each stage
                # resolve input and output feature path
                stage.code_base_dir = Path(self.CODE_DIR)
                stage.feat_base_dir = Path(self.FEAT_DIR)

                # Pack the stage object and all its dependencies to `code_dir`
                file_dict = stage.serialize()
                for file_path, file_bytes in file_dict.items():
                    with open(file_path, 'wb') as f:
                        f.write(file_bytes)

                # manage stages by dvc
                # dvc stage add -n <stage name> -d stage.py -d input.csv -O output.csv -w self.workdir python stage.py
                cmd = [
                    'dvc', 'stage', 'add', '-f', '-n', str(stage.name),
                    '-O', stage.outputs.path, '-w', str(self.workdir),
                ]
                # Add dependencies
                for dependency in stage.dependency:
                    dependency = Path(dependency)
                    cmd += ['-d', str(dependency.relative_to(self.workdir) if dependency.is_absolute() else dependency)]
                # Add running command
                cmd += ['python', os.path.join(self.CODE_DIR, f'{stage.name}.py')]
                subprocess.call(cmd)

    def __call__(self):
        # dvc repo
        cmd = ['dvc', 'repro', str(self.workdir / 'dvc.yaml')]
        subprocess.run(cmd)
