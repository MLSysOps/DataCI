#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Mar 11, 2023
"""
from . import db_connection


def create_one_dataset(dataset_dict):
    pipeline_dict = dataset_dict['yield_pipeline']
    with db_connection:
        db_connection.execute(
            """
            INSERT INTO dataset (name, version, yield_pipeline_name, yield_pipeline_version, log_message, timestamp, 
            filename, file_config, parent_dataset_name, parent_dataset_version)
            VALUES (?,?,?,?,?,?,?,?,?,?)
            ;
            """,
            (
                dataset_dict['name'], dataset_dict['version'], pipeline_dict['name'], pipeline_dict['version'],
                dataset_dict['log_message'], dataset_dict['timestamp'], dataset_dict['filename'],
                dataset_dict['file_config'], dataset_dict['parent_dataset_name'],
                dataset_dict['parent_dataset_version'],
            )
        )


def get_one_dataset(name, version='latest'):
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
                ;
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
                ;
                """, (name,))
    dataset_po_list = list(dataset_po_iter)
    if len(dataset_po_list) == 0:
        raise ValueError(f'Dataset {name}@{version} not found.')
    if len(dataset_po_list) > 1:
        raise ValueError(f'Found more than one dataset {name}@{version}.')
    name, version, yield_pipeline_name, yield_pipeline_version, log_message, timestamp, filename, file_config, \
        parent_dataset_name, parent_dataset_version = dataset_po_list[0]
    return {
        'name': name, 'version': version,
        'yield_pipeline': {'name': yield_pipeline_name, 'version': yield_pipeline_version}, 'log_message': log_message,
        'timestamp': timestamp, 'filename': filename, 'file_config': file_config,
        'parent_dataset_name': parent_dataset_name, 'parent_dataset_version': parent_dataset_version,
    }


def get_many_datasets(name, version=None):
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
            ;
            """, (name, version))
    dataset_dict_list = list()
    for dataset_po in dataset_po_iter:
        name, version, yield_pipeline_name, yield_pipeline_version, log_message, timestamp, filename, file_config, \
            parent_dataset_name, parent_dataset_version = dataset_po
        dataset_dict = {
            'name': name, 'version': version,
            'yield_pipeline': {'name': yield_pipeline_name, 'version': yield_pipeline_version},
            'log_message': log_message,
            'timestamp': timestamp, 'filename': filename, 'file_config': file_config,
            'parent_dataset_name': parent_dataset_name, 'parent_dataset_version': parent_dataset_version,
        }
        dataset_dict_list.append(dataset_dict)
    return dataset_dict_list


def get_many_dataset_update_plan(name):
    with db_connection:
        result_iter = db_connection.execute(
            """
            --- beginsql
            WITH base AS 
            (
                SELECT  version
                        ,parent_dataset_name
                        ,parent_dataset_version
                        ,yield_pipeline_name
                        ,yield_pipeline_version
                FROM    dataset
                WHERE   name = ?
            )
            ,dataset_list AS 
            (
                SELECT  dataset.*
                FROM    dataset
                JOIN    (
                            SELECT      parent_dataset_name
                            FROM        base
                            GROUP BY    parent_dataset_name
                        ) cur_dataset
                ON      cur_dataset.parent_dataset_name = name
            )
            ,pipeline_list AS 
            (
                SELECT  pipeline.*
                FROM    pipeline
                JOIN    (
                            SELECT      yield_pipeline_name
                            FROM        base
                            GROUP BY    yield_pipeline_name
                        ) 
                ON      yield_pipeline_name = name
            )
            SELECT  dataset_list.name
                    ,dataset_list.version
                    ,dataset_list.yield_pipeline_name
                    ,dataset_list.yield_pipeline_version
                    ,dataset_list.log_message
                    ,dataset_list.timestamp
                    ,dataset_list.file_config
                    ,dataset_list.filename
                    ,dataset_list.parent_dataset_name
                    ,dataset_list.parent_dataset_version
                    ,pipeline_list.name
                    ,pipeline_list.version
                    ,pipeline_list.timestamp
            FROM    dataset_list
            CROSS JOIN pipeline_list
            LEFT JOIN base
            ON      dataset_list.name = base.parent_dataset_name
            AND     dataset_list.version = base.parent_dataset_version
            AND     pipeline_list.name = base.yield_pipeline_name
            AND     pipeline_list.version = base.yield_pipeline_version
            WHERE   base.version IS NULL
            ;
            -- endsql
            """,
            (name,),
        )

    update_plans = list()
    for result in result_iter:
        name, version, yield_pipeline_name, yield_pipeline_version, log_message, timestamp, file_config, filename, \
            parent_dataset_name, parent_dataset_version = result[:10]
        dataset_dict = {
            'name': name, 'version': version,
            'yield_pipeline': {'name': yield_pipeline_name, 'version': yield_pipeline_version},
            'log_message': log_message,
            'timestamp': timestamp, 'filename': filename, 'file_config': file_config,
            'parent_dataset_name': parent_dataset_name, 'parent_dataset_version': parent_dataset_version,
        }
        name, version, timestamp = result[10:]
        pipeline_dict = {
            'name': name, 'version': version, 'timestamp': timestamp,
        }
        update_plans.append({
            'parent_dataset': dataset_dict,
            'pipeline': pipeline_dict,
        })
    return update_plans
