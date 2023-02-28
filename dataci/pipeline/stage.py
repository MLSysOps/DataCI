#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Feb 20, 2023
"""
import inspect
import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Iterable

import pandas as pd

from dataci.dataset import list_dataset
from dataci.repo import Repo

logger = logging.getLogger(__name__)


class DataPath(object):
    def __init__(self, name, repo=None):
        self.name = name
        self.type = None
        self.path = None
        self._repo = repo or Repo()
        self.resolve_path()

    def resolve_path(self):
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
                # dataset_files.symlink_to(self.name, target_is_directory=True)
                return
        except ValueError:
            pass
        # try: input is a local file
        logger.debug(f'Assume data path {value} as local file')

        self.path = value
        self.type = new_type


class Stage(ABC):
    def __init__(self, name: str, inputs: str, outputs: str, dependency='auto', repo=None) -> None:
        self.repo = repo or Repo()
        self.name = name
        self._inputs = DataPath(inputs)
        self._outputs = DataPath(outputs)
        self._dependency = dependency
        if self._dependency != 'auto' and not isinstance(self._dependency, Iterable):
            self._dependency = [self._dependency]

    @property
    def dependency(self):
        # Resolve dependencies
        # TODO: also record the original naming? instead of file path
        if self._dependency == 'auto':
            return [self.inputs, self.outputs, inspect.getfile(self.__class__)]
        else:
            return self._dependency

    @property
    def inputs(self):
        """Resolve shortcut init argument"""
        return self._inputs.path

    @property
    def outputs(self):
        """Resolve shortcut init argument"""
        return self._outputs.path

    @abstractmethod
    def run(self, inputs):
        raise NotImplementedError('Method `run` not implemented.')

    def output_serializer(self, outputs, dest):
        """Output serializer to save the output data/feature."""
        if outputs is None:
            return
        if isinstance(outputs, pd.DataFrame):
            logger.info(f'Save output to {dest}')
            outputs.to_csv(dest, index=False)
        else:
            raise ValueError(f'Not support output object type: {type(outputs)}.')

    def __call__(self):
        outputs = self.run(str(self.inputs))
        self.output_serializer(outputs, str(self.outputs))
