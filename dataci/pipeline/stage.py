#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Feb 20, 2023
"""
import inspect
import logging
import os
import pickle
from abc import ABC, abstractmethod
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd

from dataci.dataset import Dataset
from dataci.db.curd import get_one_dataset
from dataci.repo import Repo

if TYPE_CHECKING:
    from typing import Iterable, List

logger = logging.getLogger(__name__)


class Stage(ABC):
    def __init__(
            self, name: str, inputs: str, outputs: str, dependency='auto',
            repo=None, code_base_dir=os.curdir, feat_base_dir=os.curdir
    ) -> None:
        self.repo = repo or Repo()
        self.code_base_dir = Path(code_base_dir)
        self.feat_base_dir = Path(feat_base_dir)
        self.name = name
        self._inputs = inputs
        self._outputs = outputs
        if dependency != 'auto' and not isinstance(dependency, Iterable):
            self._dependency = [dependency]
        else:
            self._dependency = dependency

    @property
    def inputs(self) -> 'Dataset':
        prev_deps = getattr(self, '__inputs_deps', None)
        current_deps = (self._inputs, str(self.feat_base_dir))
        if prev_deps != current_deps:
            try:
                # If input is a published dataset
                inputs = get_one_dataset(name=self._inputs, repo=self.repo)
            except ValueError:
                # input is an intermedia feature
                inputs = self.feat_base_dir / self._inputs
            setattr(self, '__inputs_deps', current_deps)
            setattr(self, '__inputs_cache', inputs)
        return getattr(self, '__inputs_cache')

    @property
    def outputs(self) -> 'Dataset':
        prev_deps = getattr(self, '__outputs_deps', None)

        current_deps = (self._outputs, str(self.feat_base_dir))
        if prev_deps != current_deps:
            try:
                # If input is a published dataset
                outputs = get_one_dataset(name=self._outputs)
            except ValueError:
                # input is an intermedia feature, pack it into a dataset object
                outputs = self.feat_base_dir / self._outputs
            setattr(self, '__outputs_deps', current_deps)
            setattr(self, '__outputs_cache', outputs)
        return getattr(self, '__outputs_cache')

    @property
    def dependency(self):
        # Resolve dependencies
        if self._dependency == 'auto':
            return [self.inputs] + self.__get_serialize_files()
        else:
            return self._dependency

    @abstractmethod
    def run(self, inputs) -> 'List[Path]':
        raise NotImplementedError('Method `run` not implemented.')

    def __get_serialize_files(self):
        return [
            (self.code_base_dir / self.name).with_suffix('.pkl'),
            (self.code_base_dir / self.name).with_suffix('.py')
        ]

    def serialize(self):
        """Pack current stage object to re-buildable files

        TODO: Pack using code instead of pickle
        """
        import cloudpickle

        file_dict = dict()
        file_dict[(self.code_base_dir / self.name).with_suffix('.pkl')] = cloudpickle.dumps(self)
        # Generate run stage execution file
        file_dict[(self.code_base_dir / self.name).with_suffix('.py')] = inspect.cleandoc(
            f"""import pickle
            from pathlib import Path


            with open(Path(__file__).with_suffix('.pkl'), 'rb') as f:
                stage_obj = pickle.load(f)
            stage_obj()
            """
        ).encode()
        return file_dict

    @staticmethod
    def deserialize(entry_file):
        """Deserialize stage object from file or byte string
        TODO: Load from code instead of pickle
        """
        obj_pkl = Path(entry_file).with_suffix('.pkl')
        with open(obj_pkl, 'rb') as f:
            stage_obj: Stage = pickle.load(f)
            return stage_obj

    def input_deserializer(self, inputs):
        if os.path.splitext(inputs)[-1] == '.csv':
            logger.info(f'Load input {inputs} as pandas Dataframe')
            return pd.read_csv(inputs)
        else:
            raise ValueError(f'Not support input file type: {inputs}')

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
        inputs = self.input_deserializer(self.inputs.dataset_files)
        outputs = self.run(inputs)
        self.output_serializer(outputs, str(self.outputs.dataset_files))
