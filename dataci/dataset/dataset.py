#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Mar 10, 2023
"""
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd

from dataci.connector.s3 import download as s3_download
from dataci.workspace import Workspace

if TYPE_CHECKING:
    from typing import Optional, Union
    from dataci.workflow.pipeline import Pipeline


class Dataset(object):
    from .save import save  # type: ignore[misc]
    from .update import update  # type: ignore[misc]
    from .tag import tag  # type: ignore[misc]
    from .list import get  # type: ignore[misc]
    get = classmethod(get)

    def __init__(
            self,
            name,
            dataset_files=None,
            yield_pipeline: 'Optional[Union[Pipeline, dict]]' = None,
            parent_dataset: 'Optional[Union[Dataset, dict]]' = None,
            log_message=None,
            id_column='id',
            **kwargs,
    ):
        # Get workspace name from provided name or default workspace
        workspace_name, dataset_name = name.split('.') if '.' in name else (None, name)
        self.workspace = Workspace(workspace_name)
        self.name = dataset_name
        # Cache dataset files from cloud object storage
        if dataset_files is not None:
            # dataset_files is a S3 path
            # FIXME: only support single file
            if dataset_files.startswith('s3://'):
                # Download to local cache directory
                # FIXME: same file will be overwritten
                cache_dir = self.workspace.tmp_dir
                cache_path = cache_dir / dataset_files.split('/')[-1]
                s3_download(dataset_files, str(cache_dir))
                self.dataset_files = cache_path
            else:
                # dataset_files is a local path
                self.dataset_files = Path(dataset_files)
        else:
            self.dataset_files = None
        self._yield_pipeline = yield_pipeline
        self._parent_dataset = parent_dataset
        self.log_message = log_message or ''
        # TODO: create a dataset schema and verify
        self.id_column = id_column
        self.__published = False
        self.version = None
        self.create_date: 'Optional[datetime]' = None
        # TODO: improve this get size of dataset
        if self.dataset_files and self.dataset_files.suffix == '.csv':
            self.size = len(pd.read_csv(self.dataset_files))
        else:
            self.size = None

    def __repr__(self):
        if all((self.name, self.version)):
            return str(self.name, self.version)
        else:
            return f'{self.name} ! Unpublished'

    @classmethod
    def from_dict(cls, config):
        # Build parent_dataset
        if all(config['parent_dataset'].values()):
            config['parent_dataset'] = {
                'name': config['parent_dataset_name'], 'version': config['parent_dataset_version']
            }
        else:
            config['parent_dataset'] = None
        dataset_obj = cls(**config)
        dataset_obj.create_date = datetime.fromtimestamp(config['timestamp'])
        dataset_obj.version = config['version']
        dataset_obj.dataset_files = (
                dataset_obj.workspace.data_dir / dataset_obj.name / dataset_obj.version /
                config['filename']
        )
        return dataset_obj

    def dict(self):
        yield_pipeline_dict = self.yield_pipeline.dict() if self.yield_pipeline else {'name': None, 'version': None}
        parent_dataset_dict = {'name': self.parent_dataset.name, 'version': self.parent_dataset.version} \
            if self.parent_dataset else {'name': None, 'version': None}
        config = {
            'workspace': self.workspace.name,
            'name': self.name,
            'timestamp': self.create_date.timestamp() if self.create_date else None,
            'parent_dataset': parent_dataset_dict,
            'yield_pipeline': yield_pipeline_dict,
            'log_message': self.log_message,
            'version': self.version,
            'filename': self.dataset_files.name,
            'size': self.size,
            'id_column': self.id_column,
        }
        return config

    @property
    def yield_pipeline(self):
        """Lazy load yield workflow"""
        if isinstance(self._yield_pipeline, dict):
            from dataci.workflow.pipeline import Pipeline

            self._yield_pipeline = Pipeline.from_dict(self._yield_pipeline)
        return self._yield_pipeline

    @property
    def parent_dataset(self):
        # The parent dataset is None or already loaded
        if self._parent_dataset is None or isinstance(self._parent_dataset, Dataset):
            return self._parent_dataset
        # Load the parent dataset using `list_dataset` API
        from dataci.dataset import list_dataset
        datasets = list_dataset(
            f'{self._parent_dataset["name"]}@{self._parent_dataset["version"]}',
            tree_view=False,
        )
        if len(datasets) == 0:
            self._parent_dataset = None
        else:
            self._parent_dataset = datasets[0]
        return self._parent_dataset

    def __str__(self):
        return f'{self.name}@{self.version}' if self.version else f'{self.name} ! Unpublished'

    def __hash__(self):
        return hash(str(self))

    def __eq__(self, __o: object) -> bool:
        if isinstance(__o, type(self)):
            return repr(self) == repr(__o)
        return False
