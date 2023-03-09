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
if TYPE_CHECKING:
    from typing import Iterable, List

import pandas as pd

from dataci.dataset import list_dataset
from dataci.repo import Repo

logger = logging.getLogger(__name__)


class DataPath(object):
    def __init__(self, name, repo=None, basedir: os.PathLike = os.curdir):
        self.name = name
        self.type = None
        self.path = None
        self.basedir = Path(basedir).resolve()
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
                return
        except ValueError:
            pass
        # try: input is a pipeline feat
        logger.debug(f'Try resolve data path {value} as published pipeline feat')
        try:
            pass
            # feats = list_pipeline_feat(self._repo, value, tree_view=False)
            # if len(feats) == 1:
            #     path = feats.path
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
    def inputs(self) -> DataPath:
        prev_deps = getattr(self, '__inputs_deps', None)
        current_deps = (self._inputs, str(self.feat_base_dir))
        if prev_deps != current_deps:
            inputs = DataPath(self._inputs, basedir=self.feat_base_dir)
            setattr(self, '__inputs_deps', current_deps)
            setattr(self, '__inputs_cache', inputs)
        return getattr(self, '__inputs_cache')

    @property
    def outputs(self) -> DataPath:
        prev_deps = getattr(self, '__outputs_deps', None)
        current_deps = (self._outputs, str(self.feat_base_dir))
        if prev_deps != current_deps:
            outputs = DataPath(self._outputs, basedir=self.feat_base_dir)
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
        outputs = self.run(str(self.inputs.path))
        self.output_serializer(outputs, str(self.outputs.path))
