#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Mar 09, 2023

Run for pipeline.
"""
from copy import deepcopy
from shutil import copytree, rmtree, copy2
from typing import TYPE_CHECKING

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
        # Clean all for the run workdir
        if self.workdir.exists():
            rmtree(self.workdir)
        # Create workdir folder
        self.workdir.mkdir(parents=True)
        # Link code to work directory
        (self.workdir / self.pipeline.CODE_DIR).symlink_to(
            self.pipeline.workdir / self.pipeline.CODE_DIR, target_is_directory=True
        )
        # Create feat dir and copy common feat into the feat dir
        copytree(self.pipeline.workdir / self.pipeline.FEAT_DIR, self.workdir / self.pipeline.FEAT_DIR, symlinks=True)
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

    def dict(self):
        return {'run_num': self.run_num, 'pipeline': self.pipeline.dict()}

    @classmethod
    def from_dict(cls, config):
        from dataci.pipeline.pipeline import Pipeline

        config['pipeline']['repo'] = config.get('repo', None)
        config['pipeline'] = Pipeline.from_dict(config['pipeline'])
        config['pipeline'].restore()
        return cls(**config)
