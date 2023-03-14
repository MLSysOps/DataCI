#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Mar 13, 2023
"""
from . import db_connection


def create_one_pipeline(pipeline_dict):
    with db_connection:
        # Publish pipeline
        db_connection.execute(
            """
            INSERT INTO pipeline (name, version, timestamp) VALUES (?,?,?)
            """,
            (pipeline_dict['name'], pipeline_dict['version'], pipeline_dict['timestamp']),
        )


def create_one_pipeline_run(run_dict, outputs_dict):
    with db_connection:
        pipeline_dict = run_dict['pipeline']
        # Publish run
        db_connection.execute(
            """
            INSERT INTO run(run_num, pipeline_name, pipeline_version) VALUES(?,?,?)
            """
            , (run_dict['run_num'], pipeline_dict['name'], pipeline_dict['version'])
        )

        # Publish pipeline output dataset
        for output_dataset in outputs_dict:
            db_connection.execute(
                """
                INSERT INTO dataset (name, version, yield_pipeline_name, yield_pipeline_version, log_message, 
                timestamp, filename, file_config, parent_dataset_name, parent_dataset_version)
                VALUES (?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    output_dataset['name'], output_dataset['version'], pipeline_dict['name'], pipeline_dict['version'],
                    output_dataset['log_message'], output_dataset['timestamp'], output_dataset['filename'],
                    output_dataset['file_config'], output_dataset['parent_dataset_name'],
                    output_dataset['parent_dataset_version'],
                )
            )


def get_one_pipeline(name, version='latest'):
    with db_connection:
        if version != 'latest':
            pipeline_dict_iter = db_connection.execute(
                """
                SELECT name,
                       version, 
                       timestamp
                FROM   pipeline
                WHERE  name = ?
                AND    version = ?
                """, (name, version))
        else:
            pipeline_dict_iter = db_connection.execute(
                """
                SELECT name,
                       version, 
                       timestamp
                FROM  (
                    SELECT *,
                           rank() OVER (PARTITION BY name ORDER BY timestamp DESC) AS rk
                    FROM   pipeline
                    WHERE  name = ?
                )
                WHERE rk = 1
                """, (name,))
    pipeline_dict_list = list(pipeline_dict_iter)
    if len(pipeline_dict_list) == 0:
        raise ValueError(f'Pipeline {name}@{version} not found.')
    if len(pipeline_dict_list) > 1:
        raise ValueError(f'Found more than one dataset {name}@{version}.')
    name, version, timestamp = pipeline_dict_list[0]
    return {
        'name': name, 'version': version, 'timestamp': timestamp,
    }


def get_many_pipeline(name, version):
    with db_connection:
        pipeline_dict_iter = db_connection.execute(
            """
            SELECT name,
                   version, 
                   timestamp
            FROM   pipeline
            WHERE  name GLOB ?
            AND    version GLOB ?
            """,
            (name, version),
        )
    pipeline_dict_list = list()
    for pipeline_dict in pipeline_dict_iter:
        name, version, timestamp = pipeline_dict
        pipeline_dict = {
            'name': name, 'version': version, 'timestamp': timestamp,
        }
        pipeline_dict_list.append(pipeline_dict)
    return pipeline_dict_list