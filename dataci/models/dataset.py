#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Mar 10, 2023
"""
import hashlib
import io
import json
import logging
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd
from pandas.core.util.hashing import hash_pandas_object

from dataci.db.dataset import (
    get_many_datasets,
    get_one_dataset_by_version,
    get_one_dataset_by_tag,
    get_next_dataset_version_tag,
    create_one_dataset,
    create_one_dataset_tag,
    exist_dataset_by_version
)
from dataci.utils import hash_binary
from .base import BaseModel

if TYPE_CHECKING:
    from typing import Optional

logger = logging.getLogger(__name__)


class Dataset(BaseModel):

    def __init__(
            self,
            name,
            dataset_files=None,
            id_column='id',
            **kwargs,
    ):
        super().__init__(name, **kwargs)
        self._dataset_files = None
        if dataset_files is not None:
            self.dataset_files = dataset_files
        # TODO: create a dataset schema and verify
        self.id_column = id_column
        self.__published = False
        self.create_date: 'Optional[datetime]' = None
        self.size = None

    @property
    def dataset_files(self) -> 'Optional[DataFile]':
        return self._dataset_files

    @dataset_files.setter
    def dataset_files(self, value):
        # dataset_files is a S3 path
        # FIXME: only support single file
        if isinstance(value, (str, Path)) and str(value).startswith('s3://'):
            from dataci.connector.s3 import download as s3_download

            # Download to local cache directory
            # FIXME: same file will be overwritten
            cache_dir = self.workspace.tmp_dir
            cache_path = cache_dir / str(value).split('/')[-1]
            s3_download(value, str(cache_dir))
            self._dataset_files = DataFile(cache_path)
        else:
            # dataset_files is a local path
            self._dataset_files = DataFile(value)
        self.size = len(value)

    @classmethod
    def from_dict(cls, config):
        config['name'] = f'{config["workspace"]}.{config["name"]}'
        dataset_obj = cls(**config)
        dataset_obj.create_date = datetime.fromtimestamp(config['timestamp'])
        dataset_obj.version = config['version']
        dataset_obj.dataset_files = config['location']
        dataset_obj.size = config['size']
        return dataset_obj

    def dict(self, id_only=False):
        config = {
            'workspace': self.workspace.name,
            'name': self.name,
            'version': self.version,
            'version_tag': self.version_tag,
            'log_message': '',  # FIXME: not used
            'timestamp': self.create_date.timestamp() if self.create_date else None,
            'location': self.dataset_files.location if self.dataset_files is not None else None,
            'size': self.size,
            'id_column': self.id_column,
        }
        return config

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
            'datasets': self.dataset_files.md5,
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

        # If the dataset version is not in DB, save dataset
        dataset_cache_dir = self.workspace.data_dir / self.name / version
        dataset_cache_dir.mkdir(parents=True, exist_ok=True)
        # File extension will be automatically resolved when save, so we put a dummy file extension
        self.dataset_files.save(dataset_cache_dir / 'file.auto')
        print(self.dataset_files.location)

        # Get config after call save the dataset file, since the dataset file location is not known before save
        config = self.dict()
        config['version'] = version
        # Update create date
        config['timestamp'] = int(datetime.now().timestamp())
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


class DataFile(object):
    def __init__(self, file):
        self.location: Optional[str] = None
        self._file = None

        if isinstance(file, (str, Path)):
            self.location = str(file)
        else:
            self._file = file

    def __del__(self):
        if isinstance(self._file, io.TextIOWrapper):
            self._file.close()

    def __len__(self):
        return len(self.read())

    def read(self):
        if self._file is not None:
            return self._file
        if self.location.endswith('.csv'):
            # read csv
            self._file = pd.read_csv(self.location)
        elif self.location.endswith('.parquet'):
            # read parquet
            self._file = pd.read_parquet(self.location)
        else:
            raise ValueError(f'Unable to read file {self.location}')
        return self._file

    def save(self, path=None):
        """Save the loaded file to specified location"""
        # file is a pandas dataframe, serialize it to parquet
        if isinstance(self._file, pd.DataFrame):
            # change the file extension to parquet
            path = Path(path or self.location).with_suffix('.parquet')
            self._file.to_parquet(path, engine='fastparquet', compression='gzip')
        # unknown file type
        else:
            raise ValueError(f'Unable to serialize dataset_files type {type(self._file)}')
        self.location = str(path)

    @property
    def schema(self):
        pass

    @property
    def md5(self):
        return hashlib.md5(
            hash_pandas_object(self.read()).values
        ).hexdigest()
