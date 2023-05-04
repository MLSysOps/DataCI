#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Mar 11, 2023
"""
from . import db_connection


def create_one_dataset(dataset_dict):
    workflow_dict = dataset_dict['yield_workflow']
    parent_dataset_dict = dataset_dict['parent_dataset']
    with db_connection:
        db_connection.execute(
            """
            INSERT INTO dataset ( workspace
                                , name
                                , version
                                , yield_workflow_workspace
                                , yield_workflow_name
                                , yield_workflow_version
                                , log_message
                                , timestamp
                                , id_column
                                , size
                                , filename
                                , parent_dataset_workspace
                                , parent_dataset_name
                                , parent_dataset_version)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ;
            """,
            (
                dataset_dict['workspace'],
                dataset_dict['name'],
                dataset_dict['version'],
                workflow_dict['workspace'],
                workflow_dict['name'],
                workflow_dict['version'],
                dataset_dict['log_message'],
                dataset_dict['timestamp'],
                dataset_dict['id_column'],
                dataset_dict['size'],
                dataset_dict['filename'],
                parent_dataset_dict['workspace'],
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


def exists_dataset(workspace, name, version):
    with db_connection:
        cur = db_connection.cursor()
        cur = cur.execute(
            """
            SELECT 1
            FROM   dataset
            WHERE  workspace = ?
            AND    name = ?
            AND    version = ?
            ;
            """,
            (workspace, name, version)
        )
        return cur.fetchone() is not None


def update_one_dataset(dataset_dict):
    yield_workflow_dict = dataset_dict['yield_workflow']
    parent_dataset_dict = dataset_dict['parent_dataset']
    with db_connection:
        cur = db_connection.cursor()
        cur.execute(
            """
            UPDATE dataset
            SET    yield_workflow_workspace = ?
            ,      yield_workflow_name = ?
            ,      yield_workflow_version = ?
            ,      log_message = ?
            ,      timestamp = ?
            ,      id_column = ?
            ,      size = ?
            ,      filename = ?
            ,      parent_dataset_workspace = ?
            ,      parent_dataset_name = ?
            ,      parent_dataset_version = ?
            WHERE  workspace = ?
            AND    name = ?
            AND    version = ?
            ;
            """,
            (
                yield_workflow_dict['workspace'],
                yield_workflow_dict['name'],
                yield_workflow_dict['version'],
                dataset_dict['log_message'],
                dataset_dict['timestamp'],
                dataset_dict['id_column'],
                dataset_dict['size'],
                dataset_dict['filename'],
                parent_dataset_dict['workspace'],
                parent_dataset_dict['name'],
                parent_dataset_dict['version'],
                dataset_dict['workspace'],
                dataset_dict['name'],
                dataset_dict['version'],
            )
        )
        return cur.rowcount


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
                     , yield_workflow_workspace
                     , yield_workflow_name
                     , yield_workflow_version
                     , log_message
                     , timestamp
                     , id_column
                     , size
                     , filename
                     , parent_dataset_workspace
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
                       yield_workflow_workspace,
                       yield_workflow_name,
                       yield_workflow_version,
                       log_message,
                       timestamp,
                       id_column,
                       size,
                       filename,
                       parent_dataset_workspace,
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
    workspace, name, version, yield_workflow_workspace, yield_workflow_name, yield_workflow_version, log_message, \
    timestamp, id_column, size, filename, parent_dataset_workspace, parent_dataset_name, parent_dataset_version = \
        dataset_po_list[0]
    return {
        'workspace': workspace, 'name': name, 'version': version,
        'yield_workflow': {
            'workspace': yield_workflow_workspace,
            'name': yield_workflow_name,
            'version': yield_workflow_version,
        },
        'log_message': log_message,
        'timestamp': timestamp, 'size': size, 'filename': filename,
        'parent_dataset': {
            'workspace': parent_dataset_workspace,
            'name': parent_dataset_name,
            'version': parent_dataset_version,
        },
    }


def get_many_datasets(workspace, name, version=None):
    with db_connection:
        dataset_po_iter = db_connection.execute("""
            --beginsql
            WITH selected_dataset AS (
                -- SELECT dataset_name    AS name,                      
                --        dataset_version AS version
                -- FROM  dataset_tag
                -- WHERE tag_name GLOB ?
                -- AND   tag_version GLOB ?
                -- UNION ALL
                SELECT workspace
                     , name
                     , version
                FROM   dataset
                WHERE  workspace = ?
                AND    name GLOB ?
                AND    version GLOB ?
            )
            SELECT d.workspace,
                   d.name,
                   d.version,
                   yield_workflow_workspace,
                   yield_workflow_name,
                   yield_workflow_version,
                   log_message,
                   timestamp,
                   id_column,
                   size,
                   filename,
                   parent_dataset_workspace,
                   parent_dataset_name,
                   parent_dataset_version
            FROM   dataset d
            JOIN  selected_dataset sd
            ON    d.workspace = sd.workspace
            AND   d.name = sd.name
            AND   d.version = sd.version
            ;
            --endsql
            """, (workspace, name, version))
    dataset_dict_list = list()
    for dataset_po in dataset_po_iter:
        dataset_dict = {
            'workspace': dataset_po[0],
            'name': dataset_po[1],
            'version': dataset_po[2],
            'yield_workflow': {
                'workspace': dataset_po[3],
                'name': dataset_po[4],
                'version': dataset_po[5],
            },
            'log_message': dataset_po[6],
            'timestamp': dataset_po[7],
            'id_column': dataset_po[8],
            'size': dataset_po[9],
            'filename': dataset_po[10],
            'parent_dataset': {
                'workspace': dataset_po[11],
                'name': dataset_po[12],
                'version': dataset_po[13],
            },
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
            AND    length(version) < 32
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
                        ,yield_workflow_name
                        ,yield_workflow_version
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
            ,workflow_list AS 
            (
                SELECT  workflow.*
                FROM    workflow
                JOIN    (
                            SELECT      yield_workflow_name
                            FROM        base
                            GROUP BY    yield_workflow_name
                        ) 
                ON      yield_workflow_name = name
            )
            SELECT  dataset_list.name
                    ,dataset_list.version
                    ,dataset_list.yield_workflow_name
                    ,dataset_list.yield_workflow_version
                    ,dataset_list.log_message
                    ,dataset_list.timestamp
                    ,dataset_list.id_column
                    ,dataset_list.size
                    ,dataset_list.filename
                    ,dataset_list.parent_dataset_name
                    ,dataset_list.parent_dataset_version
                    ,workflow_list.name
                    ,workflow_list.version
                    ,workflow_list.timestamp
            FROM    dataset_list
            CROSS JOIN workflow_list
            LEFT JOIN base
            ON      dataset_list.name = base.parent_dataset_name
            AND     dataset_list.version = base.parent_dataset_version
            AND     workflow_list.name = base.yield_workflow_name
            AND     workflow_list.version = base.yield_workflow_version
            WHERE   base.version IS NULL
            ;
            -- endsql
            """,
            (name,),
        )

    update_plans = list()
    for result in result_iter:
        name, version, yield_workflow_name, yield_workflow_version, log_message, timestamp, id_column, size, \
        file_config, filename, parent_dataset_name, parent_dataset_version = result[:12]
        dataset_dict = {
            'name': name, 'version': version,
            'yield_workflow': {'name': yield_workflow_name, 'version': yield_workflow_version},
            'log_message': log_message, 'timestamp': timestamp, 'id_column': id_column, 'size': size,
            'filename': filename, 'file_config': file_config,
            'parent_dataset_name': parent_dataset_name, 'parent_dataset_version': parent_dataset_version,
        }
        name, version, timestamp = result[12:]
        workflow_dict = {
            'name': name, 'version': version, 'timestamp': timestamp,
        }
        update_plans.append({
            'parent_dataset': dataset_dict,
            'workflow': workflow_dict,
        })
    return update_plans
