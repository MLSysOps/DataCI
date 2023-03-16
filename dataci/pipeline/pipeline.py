#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Feb 23, 2023
"""
import os
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Iterable, Union, Optional

import yaml

from dataci.dataset.dataset import Dataset
from dataci.repo import Repo
from dataci.run import Run
from dataci.utils import cwd, symlink_force
from .stage import Stage
from .utils import generate_pipeline_version_id


class Pipeline(object):
    CODE_DIR = 'code'
    FEAT_DIR = 'feat'
    RUN_DIR = 'runs'

    from .publish import publish  # type: ignore[misc]
    from .get import get_next_run_num  # type: ignore[misc]

    def __init__(
            self,
            name: str,
            version: str = None,
            basedir: os.PathLike = os.curdir,
            repo: Repo = None,
            stages: Union[Iterable[Stage], Stage] = None,
            **kwargs,
    ):
        self.repo = repo or Repo()
        self.name = name
        # Filled version if pipeline published
        self.version = version or 'latest'
        self.create_date: 'Optional[datetime]' = datetime.now()
        self.basedir = Path(basedir).resolve()
        # latest is regard as this pipeline is not published
        self.is_built = (self.version != 'latest')

        # stages
        self.stages = stages or list()
        if not isinstance(self.stages, Iterable):
            self.stages = [self.stages]
        self.workdir = self.basedir / self.name / self.version

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
        output_list = list(outputs - inputs)
        # Pack output to a dataset
        output_datasets = list()
        for outputs in output_list:
            if not isinstance(outputs, Dataset):
                outputs = Dataset(
                    name=f'{self.name}:{outputs.stem}', repo=self.repo,
                    dataset_files=outputs, yield_pipeline=self, parent_dataset=self.inputs[0],
                )
            output_datasets.append(outputs)
        return output_datasets

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

                # Get output path
                output_path = str(stage.outputs.dataset_files if isinstance(stage.outputs, Dataset) else stage.outputs)

                # manage stages by dvc
                # dvc stage add -n <stage name> -d stage.py -d input.csv -O output.csv -w self.workdir python stage.py
                cmd = [
                    'dvc', 'stage', 'add', '-f', '-n', str(stage.name),
                    '-o', output_path, '-w', str(self.workdir),
                ]
                # Add dependencies
                for dependency in stage.dependency:
                    if isinstance(dependency, Dataset):
                        # Link global dataset files path to local
                        local_file_path = os.path.join(self.FEAT_DIR, dependency.name + dependency.dataset_files.suffix)
                        symlink_force(dependency.dataset_files, local_file_path)
                        dependency = local_file_path
                        print(local_file_path)
                    else:
                        dependency = os.path.relpath(str(dependency), str(self.workdir))
                    cmd += ['-d', dependency]
                # Add running command
                cmd += ['python', os.path.join(self.CODE_DIR, f'{stage.name}.py')]
                subprocess.call(cmd)
            # Get pipeline version
            self.version = generate_pipeline_version_id(self.CODE_DIR)
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

    def __call__(self, auto_save=True):
        # Create a Run
        run = Run(pipeline=self, run_num=self.get_next_run_num())
        run.prepare()
        with cwd(run.workdir):
            # dvc repo
            cmd = ['dvc', 'repro', str(run.workdir / 'dvc.yaml')]
            subprocess.run(cmd)
        if auto_save:
            run.save()
        return run

    def dict(self):
        return {'name': self.name, 'version': self.version, 'timestamp': int(self.create_date.timestamp())}

    @classmethod
    def from_dict(cls, config: 'dict'):
        config['repo'] = config.get('repo', None) or Repo()
        config['basedir'] = config['repo'].pipeline_dir
        pipeline = cls(**config)
        pipeline.create_date = datetime.fromtimestamp(config['timestamp'])
        pipeline.restore()
        return pipeline

    def __str__(self):
        return f'{self.name}@{self.version[:7]}'
