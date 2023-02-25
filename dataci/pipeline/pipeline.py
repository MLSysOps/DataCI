#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: FebK 23, 2023
"""
import inspect
import os
import subprocess
from pathlib import Path

import cloudpickle

from dataci.repo import Repo
from .stage import Stage
from .utils import cwd


class Pipeline(object):
    def __init__(self, repo: Repo, name, version=None, basedir=os.curdir):
        self.repo = repo
        self.name = name
        self.version = version or 'latest'
        self.basedir = Path(basedir)

        # prepare working directory
        self.workdir = (self.basedir / self.name / self.version).resolve()
        self.code_dir = self.workdir / 'code'
        self.feat_dir = self.workdir / 'feat'
        self.workdir.mkdir(exist_ok=True, parents=True)
        self.code_dir.mkdir(exist_ok=True)
        self.feat_dir.mkdir(exist_ok=True)

        # stages
        self.stages = list()

    def add_stage(self, stage: Stage):
        self.stages.append(stage)

    def build(self):
        with cwd(self.workdir):
            for stage in self.stages:
                # For each stage
                # resolve input and output feature path
                stage.inputs = str(self.feat_dir / stage.inputs)
                stage.outputs = str(self.feat_dir / stage.outputs)

                # Pack the stage object and all its dependencies to `code_dir`
                with open((self.code_dir / stage.name).with_suffix('.pkl'), 'wb') as f:
                    cloudpickle.dump(stage, f)
                # Generate run stage execution file
                # TODO: use reflection to obtain the run stage code
                with open((self.code_dir / stage.name).with_suffix('.py'), 'w') as f:
                    f.writelines(inspect.cleandoc(
                        f"""import pickle
                        
                        
                        with open(
                            r'{(self.code_dir / stage.name).with_suffix(".pkl").relative_to(self.workdir)}', 'rb'
                        ) as f:
                            stage_obj = pickle.load(f)
                        stage_obj.run()
                        """
                    ))

                # manage stages by dvc
                # dvc stage add -n <stage name> -d stage.py -d input.csv -O output.csv -w self.workdir python stage.py
                cmd = [
                    'dvc', 'stage', 'add', '-f', '-n', str(stage.name),  # '-d', str(stage.dependency),
                    '-O', stage.outputs, '-w', str(self.workdir),
                    'python', str((self.code_dir / f'{stage.name}.py').relative_to(self.workdir))
                ]
                subprocess.call(cmd)

    def __call__(self):
        # dvc repo
        cmd = ['dvc', 'repro', str(self.workdir / 'dvc.yaml')]
        subprocess.run(cmd)
