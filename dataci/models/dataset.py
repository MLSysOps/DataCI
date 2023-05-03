#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Mar 10, 2023
"""
import logging
import shutil
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd

from dataci.connector.s3 import download as s3_download
from dataci.db.dataset import get_many_datasets, get_one_dataset, get_next_version_id, create_one_dataset
from dataci.utils import parse_data_model_get_identifier, parse_data_model_list_identifier
from .base import BaseModel
from ..decorators.event import event

if TYPE_CHECKING:
    from typing import Optional, Union
    from dataci.models import Workflow

logger = logging.getLogger(__name__)


class Dataset(BaseModel):
    def __init__(
            self,
            name,
            dataset_files=None,
            yield_workflow: 'Optional[Union[Workflow, dict]]' = None,
            parent_dataset: 'Optional[Union[Dataset, dict]]' = None,
            log_message=None,
            id_column='id',
            **kwargs,
    ):
        super().__init__(name, **kwargs)
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
        self._yield_workflow = yield_workflow
        self._parent_dataset = parent_dataset
        self.log_message = log_message or ''
        # TODO: create a dataset schema and verify
        self.id_column = id_column
        self.__published = False
        self.create_date: 'Optional[datetime]' = None
        # TODO: improve this get size of dataset
        if self.dataset_files and self.dataset_files.suffix == '.csv':
            self.size = len(pd.read_csv(self.dataset_files))
        else:
            self.size = None

    @classmethod
    def from_dict(cls, config):
        # Build parent_dataset
        if all(config['parent_dataset'].values()):
            config['parent_dataset'] = {
                'name': config['parent_dataset_name'], 'version': config['parent_dataset_version']
            }
        else:
            config['parent_dataset'] = None
        # Build yield_workflow
        if all(config['yield_workflow'].values()):
            config['yield_workflow'] = {
                'workspace': config['workspace'], 'name': config['yield_workflow_name'],
                'version': config['yield_workflow_version']
            }
        else:
            config['yield_workflow'] = None
        config['name'] = f'{config["workspace"]}.{config["name"]}'
        dataset_obj = cls(**config)
        dataset_obj.create_date = datetime.fromtimestamp(config['timestamp'])
        dataset_obj.version = config['version']
        dataset_obj.dataset_files = (
                dataset_obj.workspace.data_dir / dataset_obj.name / dataset_obj.version /
                config['filename']
        )
        dataset_obj.size = config['size']
        return dataset_obj

    def dict(self):
        yield_workflow_dict = self.yield_workflow.dict() if self.yield_workflow else {
            'workspace': None, 'name': None, 'version': None,
        }
        parent_dataset_dict = {
            'workspace': self.workspace.name,
            'name': self.parent_dataset.name,
            'version': self.parent_dataset.version
        } if self.parent_dataset else {
            'workspace': None, 'name': None, 'version': None
        }
        config = {
            'workspace': self.workspace.name,
            'name': self.name,
            'timestamp': self.create_date.timestamp() if self.create_date else None,
            'parent_dataset': parent_dataset_dict,
            'yield_workflow': yield_workflow_dict,
            'log_message': self.log_message,
            'version': self.version,
            'filename': self.dataset_files.name,
            'size': self.size,
            'id_column': self.id_column,
        }
        return config

    @property
    def yield_workflow(self):
        """Lazy load yield models"""
        from dataci.models import Workflow

        if self._yield_workflow is None or isinstance(self._yield_workflow, Workflow):
            return self._yield_workflow

        self._yield_workflow = Workflow.get(
            name=f'{self._yield_workflow["workspace"]}.{self._yield_workflow["name"]}',
            version=self._yield_workflow['version'],
        )
        return self._yield_workflow

    @property
    def parent_dataset(self):
        # The parent dataset is None or already loaded
        if self._parent_dataset is None or isinstance(self._parent_dataset, Dataset):
            return self._parent_dataset
        # Load the parent dataset using get method
        self._parent_dataset = self.get(
            f'{self._parent_dataset["workspace"]}.{self._parent_dataset["name"]}@{self._parent_dataset["version"]}'
        )
        return self._parent_dataset

    def __repr__(self):
        if all((self.workspace.name, self.name, self.version)):
            return f'{self.workspace.name}.{self.name}@{self.version}'
        return f'{self.workspace.name}.{self.name} ! Unpublished'

    def __str__(self):
        return f'{self.workspace.name}.{self.name}@{self.version}'

    def __hash__(self):
        return hash(str(self))

    def __eq__(self, __o: object) -> bool:
        if isinstance(__o, type(self)):
            return repr(self) == repr(__o)
        return False

    def reload(self, config):
        """Reload dataset from db returned config
        """
        self.version = config['version']
        self.create_date = datetime.fromtimestamp(config['timestamp']) if config['timestamp'] else None
        return self

    @event('dataset_save')
    def save(self):
        config = self.dict()
        config['version'] = get_next_version_id(self.workspace.name, self.name)
        config['timestamp'] = int(datetime.now().timestamp())
        #####################################################################
        # Step 1: Save dataset to mount cloud object storage
        #####################################################################
        logger.info(f'Save dataset files to mounted S3: {self.workspace.data_dir}')
        dataset_cache_dir = self.workspace.data_dir / config['name'] / config['version']
        dataset_cache_dir.mkdir(parents=True, exist_ok=True)
        # Copy local files to cloud object storage
        # FIXME: only support a single file now
        shutil.copy2(self.dataset_files, dataset_cache_dir)

        #####################################################################
        # Step 2: Publish dataset to DB
        #####################################################################
        create_one_dataset(config)
        logger.info(f'Adding dataset to db: {self}')
        return self.reload(config)

    @classmethod
    def get(cls, name: str, version=None):
        workspace, name, version = parse_data_model_get_identifier(name=name, version=version)
        dataset_dict = get_one_dataset(workspace=workspace, name=name, version=version)
        return cls.from_dict(dataset_dict)

    @classmethod
    def find(cls, dataset_identifier=None, tree_view=False):
        """
        List dataset with optional dataset identifier to query.

        Args:
            dataset_identifier: Dataset name with optional version and optional split information to query.
                In this field, it supports three components in the format of dataset_name@version[split].
                - dataset name: Support glob. Default to query for all datasets.
                - version (optional): Version ID or the starting few characters of version ID. It will search
                    all matched versions of this dataset. Default to list all versions.
            tree_view (bool): View the queried dataset as a 3-level-tree, level 1 is dataset name, level 2 is split tag,
                and level 3 is version.

        Returns:
            A dict (tree_view=True, default) or a list (tree_view=False) of dataset information.
                If view as a tree, the format is {dataset_name: {split_tag: {version_id: dataset_info}}}.

        Examples:
            >>> from dataci.models import Dataset
            >>> Dataset.find()
            {'dataset1': {'1': ..., '2': ...}, 'dataset12': ...}
            >>> Dataset.find(dataset_identifier='dataset1')
            {'dataset1': {'1': ..., '2': ...}}
            >>> Dataset.find(dataset_identifier='data*')
            {'dataset1': {'1': ..., '2': ...}, 'dataset12': ...}
            >>> Dataset.find(dataset_identifier='dataset1@1')
            {'dataset1': {'1': ..., '2': ...}}
        """
        workspace, name, version = parse_data_model_list_identifier(identifier=dataset_identifier)

        dataset_dict_list = get_many_datasets(workspace=workspace, name=name, version=version)
        dataset_list = list()
        for dataset_dict in dataset_dict_list:
            dataset_list.append(cls.from_dict(dataset_dict))
        if tree_view:
            dataset_dict = defaultdict(dict)
            for dataset in dataset_list:
                dataset_dict[dataset.name][dataset.version] = dataset
            return dict(dataset_dict)

        return dataset_list
