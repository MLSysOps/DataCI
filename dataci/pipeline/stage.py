#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Feb 20, 2023
"""
import logging
from abc import ABC, abstractmethod
from distutils.dir_util import copy_tree
from pathlib import Path
from shutil import copyfile

from dataci.dataset import list_dataset
from dataci.repo import Repo

logger = logging.getLogger(__name__)


class Stage(ABC):
    def __init__(self, name: str, inputs: str, outputs: str, dependency='auto', repo=None) -> None:
        self.name = name
        self._inputs = inputs
        self.outputs = outputs
        self.dependency = dependency
        self.repo = repo or Repo()

    @property
    def inputs(self):
        """Resolve shortcut init argument"""
        logger.debug(f'Resolving stage {self.name}...')
        # Resolve input: find the dataset path
        # try: input is published dataset
        logger.debug(f'Try resolve inputs {self._inputs} as published dataset')
        datasets = list_dataset(self.repo, self._inputs, tree_view=False)
        if len(datasets) == 1:
            self._inputs = datasets[0].dataset_files
            return self._inputs

        # try: input is a local file
        logger.debug(f'Try resolve inputs {self._inputs} as local file')
        if Path(self._inputs).exists():
            return self._inputs
        raise ValueError(f'Unable to resolve inputs {self._inputs} for stage {self.name}.')

    @inputs.setter
    def inputs(self, new_inputs):
        if new_inputs == self._inputs:
            return
        # Set the inputs to a new path will trigger an auto copy inputs files to new path
        inputs = Path(self._inputs)
        if inputs.exists():
            if inputs.is_dir():
                copy_tree(str(inputs), new_inputs)
            else:
                copyfile(inputs, new_inputs)
        self._inputs = new_inputs

    def resolve_outputs(self):
        # Resolve outputs
        pass

    def resolve_dependency(self):
        # Resolve dependencies
        pass

    @abstractmethod
    def run(self):
        raise NotImplementedError('Method `run` not implemented.')
