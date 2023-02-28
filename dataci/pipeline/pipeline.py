#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: FebK 23, 2023
"""
import os
import subprocess
from pathlib import Path

from dataci.repo import Repo
from .stage import Stage
from .utils import cwd


class Pipeline(object):
    def __init__(self, name, version=None, basedir=os.curdir, repo: Repo = None):
        self.repo = repo or Repo()
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
                if stage._inputs.type == 'local':
                    stage._inputs.path = str(self.feat_dir / stage.inputs)
                elif stage._inputs.type == 'dataset':
                    sym_link_path = self.feat_dir / stage._inputs.name
                    # create a symbolic link
                    if not sym_link_path.exists():
                        os.symlink(stage.inputs, sym_link_path, target_is_directory=True)
                    stage._inputs.path = sym_link_path
                if stage._outputs.type == 'local':
                    stage._outputs.path = str(self.feat_dir / stage.outputs)
                elif stage._outputs.type == 'dataset':
                    # TODO: if dest is a dataset
                    pass

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
                    'dvc', 'stage', 'add', '-f', '-n', str(stage.name),
                    '-O', stage.outputs, '-w', str(self.workdir),
                ]
                # Add dependencies
                for dependency in stage.dependency:
                    cmd += ['-d', str(Path(dependency).relative_to(self.workdir))]
                # Add running command
                cmd += ['python', str((self.code_dir / f'{stage.name}.py').relative_to(self.workdir))]
                subprocess.call(cmd)

    def __call__(self):
        # dvc repo
        cmd = ['dvc', 'repro', str(self.workdir / 'dvc.yaml')]
        subprocess.run(cmd)
