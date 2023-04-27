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
    parent_dataset_dict = dataset_dict['parent_dataset']
    with db_connection:
        db_connection.execute(
            """
            INSERT INTO dataset (workspace, name, version, yield_pipeline_name, yield_pipeline_version, log_message, 
            timestamp, id_column, size, filename, parent_dataset_name, parent_dataset_version)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            ;
            """,
            (
                dataset_dict['workspace'],
                dataset_dict['name'], dataset_dict['version'], pipeline_dict['name'], pipeline_dict['version'],
                dataset_dict['log_message'], dataset_dict['timestamp'], dataset_dict['id_column'],
                dataset_dict['size'], dataset_dict['filename'],
                parent_dataset_dict['name'],
                parent_dataset_dict['version'],
            )
        )


def create_dataset_tag(dataset_name, dataset_version, tag_name, tag_version):
    with db_connection:
        db_connection.execute(
            """
            INSERT INTO dataset_tag (dataset_name, dataset_version, tag_name, tag_version)
            VALUES (?,?,?,?)
            ;
            """,
            (dataset_name, dataset_version, tag_name, tag_version)
        )


def get_one_dataset(workspace, name, version='latest'):
    with db_connection:
        if version != 'latest':
            dataset_po_iter = db_connection.execute(
                """
                --beginsql
                WITH selected_dataset AS (
                    -- SELECT dataset_name    AS name,                      
                    --        dataset_version AS version
                    -- FROM  dataset_tag
                    -- WHERE tag_name = ?
                    -- AND   tag_version = ?
                    -- UNION ALL
                    SELECT workspace
                         , name
                         , version
                    FROM   dataset
                    WHERE  workspace = ?
                    AND    name = ?
                    AND    version GLOB ?
                )
                SELECT d.workspace
                     , d.name
                     , d.version
                     , yield_pipeline_name
                     , yield_pipeline_version
                     , log_message
                     , timestamp
                     , id_column
                     , size
                     , filename
                     , parent_dataset_name
                     , parent_dataset_version
                FROM dataset d
                JOIN selected_dataset sd 
                ON   d.workspace = sd.workspace
                AND  d.name = sd.name 
                AND  d.version = sd.version
                ;
                --endsql
                """, (workspace, name, version))
        else:
            dataset_po_iter = db_connection.execute(
                """
                SELECT workspace,
                       name,
                       version, 
                       yield_pipeline_name,
                       yield_pipeline_version,
                       log_message,
                       timestamp,
                       id_column,
                       size,
                       filename,
                       parent_dataset_name,
                       parent_dataset_version
                FROM  (
                    SELECT *,
                           rank() OVER (PARTITION BY workspace, name ORDER BY timestamp DESC) AS rk
                    FROM   dataset
                    WHERE  workspace = ?
                    AND    name = ?
                )
                WHERE rk = 1
                ;
                """, (workspace, name,))
    dataset_po_list = list(dataset_po_iter)
    if len(dataset_po_list) == 0:
        raise ValueError(f'Dataset {workspace}.{name}@{version} not found.')
    if len(dataset_po_list) > 1:
        raise ValueError(f'Found more than one dataset {workspace}.{name}@{version}.')
    workspace, name, version, yield_pipeline_name, yield_pipeline_version, log_message, timestamp, id_column, size, \
    filename, parent_dataset_name, parent_dataset_version = dataset_po_list[0]
    return {
        'workspace': workspace, 'name': name, 'version': version,
        'yield_pipeline': {'name': yield_pipeline_name, 'version': yield_pipeline_version}, 'log_message': log_message,
        'timestamp': timestamp, 'size': size, 'filename': filename,
        'parent_dataset': {'name': parent_dataset_name, 'version': parent_dataset_version},
    }


def get_many_datasets(name, version=None):
    with db_connection:
        dataset_po_iter = db_connection.execute("""
            --beginsql
            WITH selected_dataset AS (
                SELECT dataset_name    AS name,                      
                       dataset_version AS version
                FROM  dataset_tag
                WHERE tag_name GLOB ?
                AND   tag_version GLOB ?
                UNION ALL
                SELECT name
                     , version
                FROM   dataset
                WHERE  name GLOB ?
                AND    version GLOB ?
            )
            SELECT d.name,
                   d.version, 
                   yield_pipeline_name,
                   yield_pipeline_version,
                   log_message,
                   timestamp,
                   id_column,
                   size,
                   filename,
                   parent_dataset_name,
                   parent_dataset_version
            FROM   dataset d
            JOIN  selected_dataset sd
            ON    d.name = sd.name
            AND   d.version = sd.version
            ;
            --endsql
            """, (name, version, name, version))
    dataset_dict_list = list()
    for dataset_po in dataset_po_iter:
        name, version, yield_pipeline_name, yield_pipeline_version, log_message, timestamp, id_column, size, \
        filename, file_config, parent_dataset_name, parent_dataset_version = dataset_po
        dataset_dict = {
            'name': name, 'version': version,
            'yield_pipeline': {'name': yield_pipeline_name, 'version': yield_pipeline_version},
            'log_message': log_message, 'timestamp': timestamp, 'id_column': id_column, 'size': size,
            'filename': filename, 'file_config': file_config,
            'parent_dataset_name': parent_dataset_name, 'parent_dataset_version': parent_dataset_version,
        }
        dataset_dict_list.append(dataset_dict)
    return dataset_dict_list


def get_next_version_id(workspace, name):
    with db_connection:
        result_iter = db_connection.execute(
            """
            SELECT COALESCE(MAX(version), 0) + 1
            FROM   dataset
            WHERE  workspace = ?
            AND    name = ?
            ;
            """, (workspace, name,))
        result = list(result_iter)[0][0]
        if result is None:
            return 1
        else:
            return result


def get_many_dataset_update_plan(name):
    with db_connection:
        result_iter = db_connection.execute(
            """
            --- beginsql
            WITH base AS 
            (

                SELECT  d.version
                        ,parent_dataset_name
                        ,parent_dataset_version
                        ,yield_pipeline_name
                        ,yield_pipeline_version
                FROM    dataset d
                WHERE   d.name GLOB ?
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
                    ,dataset_list.id_column
                    ,dataset_list.size
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
        name, version, yield_pipeline_name, yield_pipeline_version, log_message, timestamp, id_column, size, \
        file_config, filename, parent_dataset_name, parent_dataset_version = result[:12]
        dataset_dict = {
            'name': name, 'version': version,
            'yield_pipeline': {'name': yield_pipeline_name, 'version': yield_pipeline_version},
            'log_message': log_message, 'timestamp': timestamp, 'id_column': id_column, 'size': size,
            'filename': filename, 'file_config': file_config,
            'parent_dataset_name': parent_dataset_name, 'parent_dataset_version': parent_dataset_version,
        }
        name, version, timestamp = result[12:]
        pipeline_dict = {
            'name': name, 'version': version, 'timestamp': timestamp,
        }
        update_plans.append({
            'parent_dataset': dataset_dict,
            'pipeline': pipeline_dict,
        })
    return update_plans
