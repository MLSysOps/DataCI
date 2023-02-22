#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Feb 20, 2023
"""
import logging
from abc import ABC, abstractmethod
from pathlib import Path

import pandas as pd

from dataci.dataset import list_dataset

logger = logging.getLogger(__name__)


class Stage(ABC):
    def __init__(self, repo, name: str, inputs: str, outputs: str, dependency='auto') -> None:
        self.repo = repo
        self.name = name
        self.inputs = inputs
        self.outputs = outputs
        self.dependency = dependency

    def resolve_inputs(self, inputs):
        """Resolve shortcut init argument"""
        logger.debug(f'Resolving stage {self.name}...')
        # Resolve input: find the dataset path
        # try: input is published dataset
        logger.debug(f'Try resolve inputs {inputs} as published dataset')
        datasets = list_dataset(self.repo, inputs, tree_view=False)
        if len(datasets) == 1:
            # Load dataset version at temp folder from DVC file cache
            datasets[0].dataset_files

        # try: input is a local file
        logger.debug(f'Try resolve inputs {inputs} as local file')
        inputs = Path(inputs)
        if inputs.is_file() and inputs.exists():
            # Copy input files to temp folder
            inputs
            return
        raise ValueError(f'Unable to resolve inputs {inputs} for stage {self.name}.')

    def resolve_outputs(self):
        # Resolve outputs
        pass

    def resolve_dependency(self):
        # Resolve dependencies
        pass

    @staticmethod
    @abstractmethod
    def run(data):
        raise NotImplementedError('Method `run` not implemented.')

    def __call__(self):
        # Read input
        df = pd.read_csv(self.inputs)
        
        # Execute user override :code:`run` function
        outputs = df.apply(self.run(df), axis=1)
        
        # Dump output
        outputs.to_csv(self.outputs, index=False)
