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

import yaml

from dataci.dataset import Dataset
from dataci.repo import Repo
from .run import Run
from .stage import Stage
from .utils import cwd, generate_pipeline_version_id


class Pipeline(object):
    CODE_DIR = 'code'
    FEAT_DIR = 'feat'
    RUN_DIR = 'runs'

    from .publish import publish  # type: ignore[misc]

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
        # Filled version if pipeline published
        self.version = version or 'latest'
        self.basedir = Path(basedir)
        # latest is regard as this pipeline is not published
        self.is_built = (version != 'latest')

        # prepare working directory
        self.workdir = (self.basedir / self.name / self.version).resolve()

        # stages
        self.stages = stages or list()
        if not isinstance(self.stages, Iterable):
            self.stages = [self.stages]

    @property
    def inputs(self):
        # Get all inputs and outputs
        inputs, outputs = set(), set()
        for stage in self.stages:
            inputs.add(stage.inputs)
            outputs.add(stage.outputs)
        # Cancel all inputs that come from some stage's outputs
        return list(inputs - outputs)

    @property
    def outputs(self):
        # Get all inputs and outputs
        inputs, outputs = set(), set()
        for stage in self.stages:
            inputs.add(stage.inputs)
            outputs.add(stage.outputs)
        # Cancel all outputs that come from some stage's inputs
        return list(outputs - inputs)

    @property
    def runs(self):
        runs = dict()
        for run_num in (self.workdir / self.RUN_DIR).glob('*'):
            runs[run_num.stem] = Run(self, run_num)
        return runs

    def add_stage(self, stage: Stage):
        self.stages.append(stage)

    def build(self):
        self.workdir.mkdir(exist_ok=True, parents=True)
        (self.workdir / self.CODE_DIR).mkdir(exist_ok=True)
        (self.workdir / self.FEAT_DIR).mkdir(exist_ok=True)

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

                # Get pipeline version
                self.version = generate_pipeline_version_id(self.CODE_DIR)

                # Get output path
                output_path = str(stage.outputs.dataset_files if isinstance(stage.outputs, Dataset) else stage.outputs)

                # manage stages by dvc
                # dvc stage add -n <stage name> -d stage.py -d input.csv -O output.csv -w self.workdir python stage.py
                cmd = [
                    'dvc', 'stage', 'add', '-f', '-n', str(stage.name),
                    '-O', output_path, '-w', str(self.workdir),
                ]
                # Add dependencies
                for dependency in stage.dependency:
                    if isinstance(dependency, Dataset):
                        dependency = str(dependency.dataset_files)
                    else:
                        dependency = os.path.relpath(str(dependency), str(self.workdir))
                    cmd += ['-d', dependency]
                # Add running command
                cmd += ['python', os.path.join(self.CODE_DIR, f'{stage.name}.py')]
                subprocess.call(cmd)
        self.is_built = True

    def restore(self):
        # Restore all stages from code dir
        with cwd(self.workdir):
            with open('dvc.yaml') as f:
                pipeline_dvc_config: dict = yaml.safe_load(f)
            stage_names = list(pipeline_dvc_config['stages'].keys())
            for stage_name in stage_names:
                stage = Stage.deserialize(os.path.join(self.CODE_DIR, f'{stage_name}.py'))
                self.add_stage(stage)

    def __call__(self):
        # dvc repo
        cmd = ['dvc', 'repro', str(self.workdir / 'dvc.yaml')]
        subprocess.run(cmd)

    def dict(self):
        return {'name': self.name, 'version': self.version}
