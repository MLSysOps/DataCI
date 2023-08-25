#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Mar 10, 2023
"""
import abc
import hashlib
import json
import logging
import re
import shutil
import tempfile
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
    create_one_dataset,
    create_one_dataset_tag,
    exist_dataset_by_version
)
from dataci.decorators.event import event
from dataci.utils import hash_binary
from .base import BaseModel

if TYPE_CHECKING:
    from typing import Optional, Union, Type

logger = logging.getLogger(__name__)


class DataFileIO(abc.ABC):
    DEFAULT_SUFFIX: str
    NAME: str

    def __init__(self, file_path):
        self.file_path = file_path

    @abc.abstractmethod
    def read(self, num_records=None):
        pass

    @abc.abstractmethod
    def write(self, records, indices=None):
        pass

    @abc.abstractmethod
    def seek(self, offset, whence=0):
        pass

    @property
    @abc.abstractmethod
    def sha256(self):
        pass

    @abc.abstractmethod
    def __len__(self):
        pass


class CSVFileIO(DataFileIO):
    DEFAULT_SUFFIX = '.csv'
    NAME = 'csv'

    def __init__(self, file_path):
        super().__init__(file_path)
        self._pos = 0
        self._len = None
        self._sha256 = None

    def read(self, num_records=None):
        records = pd.read_csv(self.file_path, skiprows=self._pos, nrows=num_records)
        # recover the file pointer to the correct position (pos + num_records)
        if num_records is not None:
            self._pos += num_records
        return records

    def write(self, records, indices=None):
        if not isinstance(records, (pd.DataFrame, pd.Series)):
            records = pd.DataFrame(records)
        records.to_csv(self.file_path, index=indices)
        # update the file length
        self._len = len(records)
        self._sha256 = None

    def seek(self, offset, whence=0):
        # skip offset lines by dummy read 0 rows. Use engine='python' to avoid unpredictable file pointer
        # https://stackoverflow.com/a/62142109
        if whence == 0:
            self._pos = offset
        elif whence == 1:
            self._pos += offset
        elif whence == 2:
            self._pos = len(self) - offset
        else:
            raise ValueError(f'Invalid whence {whence}, only 0, 1, 2 are supported')

    @property
    def sha256(self):
        if self._sha256 is None:
            self._sha256 = hashlib.sha256(
                hash_pandas_object(self.read()).values
            ).hexdigest()
        return self._sha256

    def __len__(self):
        if self._len is None:
            return len(pd.read_csv(self.file_path))
        return self._len


class ParquetFileIO(DataFileIO):
    DEFAULT_SUFFIX = '.parquet'
    NAME = 'parquet'

    def __init__(self, file_path):
        super().__init__(file_path)
        self._pos = 0
        self._len = None
        self._sha256 = None

    def read(self, num_records=None):
        records = pd.read_parquet(self.file_path, skiprows=self._pos, nrows=num_records)
        # recover the file pointer to the correct position (pos + num_records)
        if num_records is not None:
            self._pos += num_records
        return records

    def write(self, records, indices=None):
        if not isinstance(records, (pd.DataFrame, pd.Series)):
            raise ValueError('ParquetFileIO only supports writing pandas.Series or pandas.DataFrame to parquet file')
        records.to_parquet(self.file_path, index=indices, compression='gzip')
        # update the file length
        self._len = len(records)
        self._sha256 = None

    def seek(self, offset, whence=0):
        # skip offset lines by dummy read 0 rows. Use engine='python' to avoid unpredictable file pointer
        # https://stackoverflow.com/a/62142109
        if whence == 0:
            self._pos = offset
        elif whence == 1:
            self._pos += offset
        elif whence == 2:
            self._pos = len(self) - offset
        else:
            raise ValueError(f'Invalid whence {whence}, only 0, 1, 2 are supported')

    @property
    def sha256(self):
        if self._sha256 is None:
            self._sha256 = hashlib.sha256(
                hash_pandas_object(self.read()).values
            ).hexdigest()
        return self._sha256

    def __len__(self):
        if self._len is None:
            return len(pd.read_parquet(self.file_path))
        return self._len


class AutoFileIO(DataFileIO, abc.ABC):
    NAME = 'auto'

    def __new__(cls, *args, **kwargs):
        if issubclass(cls, AutoFileIO):
            if args[0].endswith('.csv'):
                return CSVFileIO(*args, **kwargs)
            elif args[0].endswith('.parquet'):
                return ParquetFileIO(*args, **kwargs)
            else:
                raise ValueError(f'Unable to read file {args[0]}')
        else:
            return super().__new__(cls)


class SkipReaderFileIO(DataFileIO):
    NAME = 'skip'

    def __init__(self, file_path):
        super().__init__(file_path)
        # For get the file
        self.file_io = AutoFileIO(file_path)

    def read(self, num_records=None):
        return self.file_path

    def write(self, records, indices=None):
        return self.file_io.write(records, indices)

    def seek(self, offset, whence=0):
        return self.file_io.seek(offset, whence)

    @property
    def sha256(self):
        return self.file_io.sha256

    def __len__(self):
        return len(self.file_io)


file_io_registry = {
    'csv': CSVFileIO,
    'parquet': ParquetFileIO,
    'auto': AutoFileIO,
    'skip': SkipReaderFileIO,
    None: SkipReaderFileIO,
}


class Dataset(BaseModel):
    type_name = 'dataset'

    VERSION_PATTERN = re.compile(r'latest|none|\w+', flags=re.IGNORECASE)
    GET_DATA_MODEL_IDENTIFIER_PATTERN = re.compile(
        r'^(?:([a-z]\w*)\.)?([a-z]\w*)(?:@(latest|none|\w[\w-]*))?$', flags=re.IGNORECASE
    )
    LIST_DATA_MODEL_IDENTIFIER_PATTERN = re.compile(
        r'^(?:([a-z]\w*)\.)?([\w:.*[\]]+?)(?:@(latest|none|[\w*]+))?$', re.IGNORECASE
    )
    # any alphanumeric string that is not a pure 'latest', 'none', or hex string
    VERSION_TAG_PATTERN = re.compile(r'^(?!^latest$|^none$|^[\da-f]+$)\w[\w\-]*$', flags=re.IGNORECASE)

    def __init__(
            self,
            name,
            dataset_files=None,
            file_reader: 'Union[str, Type[DataFileIO], None]' = 'auto',
            file_writer: 'Union[str, Type[DataFileIO], None]' = 'csv',
            id_column='id',
            **kwargs,
    ):
        super().__init__(name, **kwargs)
        # TODO: create a dataset schema and verify
        self.id_column = id_column
        if file_reader is None or isinstance(file_reader, str):
            file_reader = file_io_registry[file_reader]
        if file_writer is None or isinstance(file_writer, str):
            file_writer = file_io_registry[file_writer]
        self.file_reader = file_reader
        self.file_writer = file_writer
        self.create_date: 'Optional[datetime]' = None
        self._dataset_files = None
        self._len = None
        self._sha256 = None

        if dataset_files is not None:
            self.dataset_files = dataset_files

    @property
    def dataset_files(self) -> 'Optional[str]':
        return self._dataset_files

    @dataset_files.setter
    def dataset_files(self, value):
        # dataset_files is a S3 path
        # FIXME: only support single file
        if isinstance(value, (str, Path)):
            if str(value).startswith('s3://'):
                from dataci.connector.s3 import download as s3_download

                # Download to local cache directory
                # FIXME: same file will be overwritten
                cache_dir = self.workspace.tmp_dir
                cache_path = cache_dir / str(value).split('/')[-1]
                s3_download(value, str(cache_dir))
                self._dataset_files = str(cache_path)
            else:
                # dataset_files is a local path
                self._dataset_files = str(value)
        else:
            # dataset_files is an object
            # Create a temp file for temporary save this dataset locally
            self._dataset_files = tempfile.NamedTemporaryFile(
                mode='w',
                suffix=self.file_writer.DEFAULT_SUFFIX,
                dir=self.workspace.tmp_dir, delete=False
            ).name
            # Write the dataset to the temp file
            self.write(value)
        self._len = None
        self._sha256 = None

    @property
    def sha256(self):
        if self._sha256 is None:
            self._sha256 = self.file_reader(self.dataset_files).sha256
        return self._sha256

    def read(self):
        # Use provided file reader class to read the dataset file
        return self.file_reader(self.dataset_files).read()

    def write(self, records=None):
        # Use provided file writer to write to the dataset file
        return self.file_writer(self.dataset_files).write(records)

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
            'location': self.dataset_files if self.dataset_files is not None else None,
            'size': len(self),
            'id_column': self.id_column,
        }
        return config

    def __len__(self):
        if self._len is None:
            self._len = len(self.file_reader(self.dataset_files))
        return self._len

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
            'workspace': self.workspace.name,
            'name': self.name,
            'datasets': self.sha256,
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
        # Copy dataset file to cache dir
        shutil.copy2(self.dataset_files, dataset_cache_dir)
        self._dataset_files = str(dataset_cache_dir / self.dataset_files.split('/')[-1])

        # Get config after call save the dataset file, since the dataset file location is not known before save
        config = self.dict()
        config['version'] = version
        # Update create date
        config['timestamp'] = int(datetime.now().timestamp())
        # Save dataset to DB
        create_one_dataset(config)
        return self.reload(config)

    @event()
    def publish(self, version_tag=None):
        self.save()
        # Check if the stage is already published
        if self.version_tag is not None:
            logger.warning(f'Dataset {self} is already published with version_tag={self.version_tag}.')
            return self
        # Check if version_tag is valid
        if version_tag is None or self.VERSION_TAG_PATTERN.match(version_tag) is None:
            raise ValueError(
                f'Dataset version_tag {version_tag} is not valid. '
                f'Expect a combination of alphanumeric, dash (-) string that is '
                f'not a pure "latest", "none" or hex string.'
            )

        config = self.dict()
        config['version_tag'] = version_tag.lower()
        create_one_dataset_tag(config)

        return self.reload(config)

    @classmethod
    def get(cls, name: str, version=None, not_found_ok=False, file_reader='auto', file_writer='csv'):
        workspace, name, version_or_tag = cls.parse_data_model_get_identifier(name, version)

        if version_or_tag is None or cls.VERSION_TAG_PATTERN.match(version_or_tag) is not None:
            # Get by tag
            config = get_one_dataset_by_tag(workspace, name, version_or_tag)
        else:
            # Get by version
            if version_or_tag.lower() == 'none':
                version_or_tag = None
            config = get_one_dataset_by_version(workspace, name, version_or_tag)

        # Check if the dataset is found
        if config is None:
            if not_found_ok:
                return None
            raise ValueError(f'Dataset {workspace}.{name}@{version_or_tag} not found.')

        config['file_reader'] = file_reader
        config['file_writer'] = file_writer
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
