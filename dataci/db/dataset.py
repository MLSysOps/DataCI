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


def get_many_datasets(workspace, name, version=None, all=False):
    with db_connection:
        if version == 'latest':
            dataset_po_iter = db_connection.execute(f"""
                --beginsql
                WITH selected_dataset AS (
                    SELECT workspace
                            , name
                            , version
                    FROM (
                        SELECT workspace
                                , name
                                , version
                                , ROW_NUMBER() OVER (PARTITION BY workspace, name ORDER BY version DESC) AS rk
                        FROM (
                            SELECT workspace
                                    , name
                                    , version
                            FROM   dataset
                            WHERE  workspace = ?
                            AND    name GLOB ?
                            AND    length(version) < 32
                        )
                    )
                    WHERE rk = 1
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
                """, (workspace, name,))
        else:
            dataset_po_iter = db_connection.execute(f"""
                --beginsql
                WITH selected_dataset AS (
                    SELECT workspace
                         , name
                         , version
                    FROM   dataset
                    WHERE  workspace = ?
                    AND    name GLOB ?
                    AND    version GLOB ?
                    {"AND    length(version) < 32" if not all else ""}
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
            SELECT COALESCE(MAX(CAST(version AS INTEGER)), 0) + 1
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
