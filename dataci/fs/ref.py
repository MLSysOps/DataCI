#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Mar 09, 2023
"""
import logging
import os
from pathlib import Path


from dataci.repo import Repo

logger = logging.getLogger(__name__)


class DataRef(object):
    def __init__(self, name, repo=None, basedir: 'os.PathLike' = os.curdir):
        self.name = name
        self.type = None
        self.path = None
        self.basedir = Path(basedir).resolve()
        self._repo = repo or Repo()
        self.resolve_path()

    def resolve_path(self):
        from dataci.dataset import list_dataset
        from dataci.pipeline.list import list_pipeline_feat

        value = self.name
        logger.debug(f'Resolving data path {value}...')
        new_type = 'local'
        # Resolve input: find the dataset path
        # try: input is published dataset
        logger.debug(f'Try resolve data path {value} as published dataset')
        try:
            datasets = list_dataset(self._repo, value, tree_view=False)
            if len(datasets) == 1:
                self.path = datasets[0].dataset_files
                self.type = 'dataset'
                return
        except ValueError:
            pass
        # try: input is a pipeline feat
        logger.debug(f'Try resolve data path {value} as published pipeline feat')
        try:
            pass
            feats = list_pipeline_feat(self._repo, value, tree_view=False)
            if len(feats) == 1:
                self.path = feats[0].path
                self.type = 'feat'
                return
        except ValueError:
            pass

        # try: input is a local file
        logger.debug(f'Assume data path {value} as local file')
        self.path = self.basedir / value
        self.type = new_type

    def rebase(self, basedir):
        basedir = Path(basedir)
        if self.type == 'local':
            relpath = self.path.relative_to(self.basedir)
            self.path = Path(basedir) / relpath
        self.basedir = basedir

    def __str__(self):
        return str(self.path)

    def __repr__(self):
        return f'DataPath(name={self.name},type={self.type},path={self.path},basedir={self.basedir})'

    def __eq__(self, other):
        return self.path == str(other)

    def __hash__(self):
        return hash(self.path)
