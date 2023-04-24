#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Mar 09, 2023

Run for pipeline.
"""
import os
from copy import deepcopy
from shutil import rmtree, copy2
from typing import TYPE_CHECKING

from dataci.utils import symlink_force

if TYPE_CHECKING:
    from dataci.pipeline.pipeline import Pipeline


class Run(object):
    from .save import save  # type: ignore[misc]
    
    def __init__(self, pipeline: 'Pipeline', run_num: int, **kwargs):
        self.pipeline = pipeline
        self.run_num = run_num

    @property
    def workdir(self):
        return self.pipeline.workdir / 'runs' / str(self.run_num)

    def prepare(self):
        from dataci.dataset import Dataset
        
        # Clean all for the run workdir
        if self.workdir.exists():
            rmtree(self.workdir)
        # Create workdir folder
        self.workdir.mkdir(parents=True)
        # Link code to work directory
        (self.workdir / self.pipeline.CODE_DIR).symlink_to(
            self.pipeline.workdir / self.pipeline.CODE_DIR, target_is_directory=True
        )
        # TODO: better way to prepare input feat
        # Create feat dir and link feat into the feat dir
        (self.workdir / self.pipeline.FEAT_DIR).mkdir(parents=True)
        for stage in self.pipeline.stages:
            for dependency in stage.dependency:
                if isinstance(dependency, Dataset):
                    # Link global dataset files path to local
                    local_file_path = os.path.join(
                        self.workdir / self.pipeline.FEAT_DIR, dependency.name + dependency.dataset_files.suffix)
                    symlink_force(dependency.dataset_files, local_file_path)
                    dependency = local_file_path

        # Copy pipeline definition file to work directory
        copy2(self.pipeline.workdir / 'dvc.yaml', self.workdir / 'dvc.yaml', )

    @property
    def feat(self):
        outputs = deepcopy(self.pipeline.outputs)
        outputs_dict = dict()
        for output in outputs:
            output.rebase(self.workdir)
            outputs_dict[output.name] = output
        return outputs_dict

    def __cmp__(self, other):
        if not isinstance(other, Run):
            raise ValueError(f'Compare between type {type(Run)} and {type(other)} is invalid.')
        if self.pipeline != other.pipeline:
            raise ValueError(
                f'Compare between two different pipeline {self.pipeline} and {other.pipeline} is invalid.'
            )
        return self.run_num.__cmp__(other.run_num)
    
    def __str__(self):
        return str(self.pipeline) + f'.run{self.run_num}'

    def dict(self):
        return {'run_num': self.run_num, 'pipeline': self.pipeline.dict()}

    @classmethod
    def from_dict(cls, config):
        from dataci.pipeline.pipeline import Pipeline

        config['pipeline']['repo'] = config.get('repo', None)
        config['pipeline'] = Pipeline.from_dict(config['pipeline'])
        config['pipeline'].restore()
        return cls(**config)
