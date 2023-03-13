#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Mar 11, 2023
"""
from dataci.dataset.dataset import Dataset

from . import db_connection


def create_one_dataset(dataset: Dataset):
    dataset_dict = dataset.dict()
    pipeline_dict = dataset_dict['yield_pipeline']
    with db_connection:
        db_connection.execute(
            """
            INSERT INTO dataset (name, version, yield_pipeline_name, yield_pipeline_version, log_message, timestamp, 
            filename, file_config, parent_dataset_name, parent_dataset_version)
            VALUES (?,?,?,?,?,?,?,?,?,?)
            """,
            (
                dataset_dict['name'], dataset_dict['version'], pipeline_dict['name'], pipeline_dict['version'],
                dataset_dict['log_message'], dataset_dict['timestamp'], dataset_dict['filename'],
                dataset_dict['file_config'], dataset_dict['parent_dataset_name'],
                dataset_dict['parent_dataset_version'],
            )
        )


def get_one_dataset(name, version='latest', repo=None):
    with db_connection:
        if version != 'latest':
            dataset_po_iter = db_connection.execute(
                """
                SELECT name,
                       version, 
                       yield_pipeline_name,
                       yield_pipeline_version,
                       log_message,
                       timestamp,
                       filename,
                       file_config,
                       parent_dataset_name,
                       parent_dataset_version
                FROM   dataset
                WHERE  name = ?
                AND    version = ?
                """, (name, version))
        else:
            dataset_po_iter = db_connection.execute(
                """
                SELECT name,
                       version, 
                       yield_pipeline_name,
                       yield_pipeline_version,
                       log_message,
                       timestamp,
                       filename,
                       file_config,
                       parent_dataset_name,
                       parent_dataset_version
                FROM  (
                    SELECT *,
                           rank() OVER (PARTITION BY name ORDER BY timestamp DESC) AS rk
                    FROM   dataset
                    WHERE  name = ?
                )
                WHERE rk = 1
                """, (name,))
    dataset_po_list = list(dataset_po_iter)
    if len(dataset_po_list) == 0:
        raise ValueError(f'Dataset {name}@{version} not found.')
    if len(dataset_po_list) > 1:
        raise ValueError(f'Found more than one dataset {name}@{version}.')
    name, version, yield_pipeline_name, yield_pipeline_version, log_message, timestamp, filename, file_config, \
    parent_dataset_name, parent_dataset_version = dataset_po_list[0]
    dataset_obj = Dataset.from_dict({
        'name': name, 'version': version,
        'yield_pipeline': {'name': yield_pipeline_name, 'version': yield_pipeline_version}, 'log_message': log_message,
        'timestamp': timestamp, 'filename': filename, 'file_config': file_config,
        'parent_dataset_name': parent_dataset_name, 'parent_dataset_version': parent_dataset_version, 'repo': repo,
    })
    return dataset_obj


def get_many_datasets(name, version=None, repo=None):
    with db_connection:
        dataset_po_iter = db_connection.execute("""
            SELECT name,
                   version, 
                   yield_pipeline_name,
                   yield_pipeline_version,
                   log_message,
                   timestamp,
                   filename,
                   file_config,
                   parent_dataset_name,
                   parent_dataset_version
            FROM   dataset
            WHERE  name GLOB ?
            AND    version GLOB ?
            """, (name, version))
    dataset_list = list()
    for dataset_po in dataset_po_iter:
        name, version, yield_pipeline_name, yield_pipeline_version, log_message, timestamp, filename, file_config, \
        parent_dataset_name, parent_dataset_version = dataset_po
        dataset_obj = Dataset.from_dict({
            'name': name, 'version': version,
            'yield_pipeline': {'name': yield_pipeline_name, 'version': yield_pipeline_version},
            'log_message': log_message,
            'timestamp': timestamp, 'filename': filename, 'file_config': file_config,
            'parent_dataset_name': parent_dataset_name, 'parent_dataset_version': parent_dataset_version, 'repo': repo,
        })
        dataset_list.append(dataset_obj)
    return dataset_list
