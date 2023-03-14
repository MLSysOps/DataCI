#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Mar 09, 2023

Run for pipeline.
"""
from copy import deepcopy
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .pipeline import Pipeline


class Run(object):
    def __init__(self, pipeline: 'Pipeline', run_num):
        self.pipeline = pipeline
        self.run_num = run_num
        self.workdir = pipeline.workdir / str(self.run_num)

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
        from .pipeline import Pipeline

        config['pipeline'] = Pipeline.from_dict(config['pipeline'])
        config['pipeline'].restore()
        return cls(**config)
