#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Mar 10, 2023
"""
import json
import logging
import shutil
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

from dataci.connector.s3 import download as s3_download
from dataci.db.dataset import (
    get_many_datasets,
    get_one_dataset_by_version,
    get_one_dataset_by_tag,
    get_next_dataset_version_tag,
    create_one_dataset,
    create_one_dataset_tag,
    exist_dataset_by_version
)
from dataci.utils import hash_file, hash_binary
from .base import BaseModel

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
        # TODO: create a dataset schema and verify
        self.id_column = id_column
        self.__published = False
        self.create_date: 'Optional[datetime]' = None
        # TODO: improve this get size of dataset
        if self.dataset_files and self.dataset_files.suffix == '.csv':
            with open(self.dataset_files) as f:
                print(self.dataset_files)
                self.size = sum(1 for _ in f) - 1  # exclude header
        else:
            self.size = None

    @classmethod
    def from_dict(cls, config):
        # Build parent_dataset
        if not all(config['parent_dataset'].values()):
            config['parent_dataset'] = None
        # Build yield_workflow
        if not all(config['yield_workflow'].values()):
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

    def dict(self, id_only=False):
        yield_workflow_dict = self.yield_workflow.dict() if self.yield_workflow else {
            'workspace': None, 'name': None, 'version': None, 'version_tag': None
        }
        parent_dataset_dict = {
            'workspace': self.workspace.name,
            'name': self.parent_dataset.name,
            'version': self.parent_dataset.version,
            'version_tag': self.parent_dataset.version_tag,
        } if self.parent_dataset else {
            'workspace': None, 'name': None, 'version': None
        }
        config = {
            'workspace': self.workspace.name,
            'name': self.name,
            'timestamp': self.create_date.timestamp() if self.create_date else None,
            'parent_dataset': parent_dataset_dict,
            'yield_workflow': yield_workflow_dict,
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

    @property
    def fingerprint(self):
        config = self.dict()
        fingerprint_dict = {
            'workspace': config['workspace'],
            'name': config['name'],
            'datasets': hash_file(self.dataset_files)
        }
        return hash_binary(json.dumps(fingerprint_dict, sort_keys=True).encode('utf-8'))

    def reload(self, config=None):
        """Reload dataset from db returned config
        """
        config = config or get_one_dataset_by_version(self.workspace.name, self.name, self.fingerprint)
        if config is None:
            return self
        self.version = config['version']
        self.version_tag = config['version_tag']
        self.create_date = datetime.fromtimestamp(config['timestamp']) if config['timestamp'] else None
        return self

    def save(self):
        """Save dataset to db with generated version ID for caching
        """
        version = self.fingerprint

        # Check if the stage is already saved
        if exist_dataset_by_version(self.workspace.name, self.name, version):
            return self.reload()

        # Check if the models name is valid
        if self.NAME_PATTERN.match(f'{self.workspace.name}.{self.name}') is None:
            raise ValueError(f'Dataset name {self.workspace}.{self.name} is not valid.')

        # Get config after call save on all stages, since the stage version might be updated
        config = self.dict()
        config['version'] = version
        # Update create date
        config['timestamp'] = int(datetime.now().timestamp())

        # If the dataset version is not in DB, save dataset
        dataset_cache_dir = self.workspace.data_dir / config['name'] / config['version']
        dataset_cache_dir.mkdir(parents=True, exist_ok=True)
        # Copy local files to cloud object storage
        # FIXME: only support a single file now
        shutil.copy2(self.dataset_files, dataset_cache_dir)
        # Save dataset to DB
        create_one_dataset(config)
        return self.reload(config)

    def publish(self):
        self.save()
        # Check if the stage is already published
        if self.version_tag is not None:
            return self
        config = self.dict()
        config['version_tag'] = str(get_next_dataset_version_tag(config['workspace'], config['name']))
        create_one_dataset_tag(config)

        return self.reload(config)

    @classmethod
    def get(cls, name: str, version=None):
        workspace, name, version_or_tag = cls.parse_data_model_get_identifier(name, version)

        if version_or_tag is None or version_or_tag == 'latest' or version_or_tag.startswith('v'):
            # Get by tag
            config = get_one_dataset_by_tag(workspace, name, version_or_tag)
        else:
            # Get by version
            if version_or_tag.lower() == 'none':
                version_or_tag = None
            config = get_one_dataset_by_version(workspace, name, version_or_tag)

        return cls.from_dict(config)

    @classmethod
    def find(cls, dataset_identifier=None, tree_view=False, all=False):
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
            all (bool): List all datasets. If False, only list published datasets. Default to False.

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
        workspace, name, version = cls.parse_data_model_list_identifier(identifier=dataset_identifier)
        # Add * at the end of version for BLOB
        if version and '*' not in version and version != 'latest':
            version += '*'

        dataset_dict_list = get_many_datasets(workspace=workspace, name=name, version=version, all=all)
        dataset_list = list()
        for dataset_dict in dataset_dict_list:
            dataset_list.append(cls.from_dict(dataset_dict))
        if tree_view:
            dataset_dict = defaultdict(dict)
            for dataset in dataset_list:
                dataset_dict[dataset.name][dataset.version] = dataset
            return dict(dataset_dict)

        return dataset_list
