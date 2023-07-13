#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Mar 11, 2023
"""
from textwrap import dedent

from . import db_connection


def create_one_dataset(dataset_dict):
    workflow_dict = dataset_dict['yield_workflow']
    parent_dataset_dict = dataset_dict['parent_dataset']
    with db_connection:
        cur = db_connection.cursor()
        cur.execute(
            dedent("""
            INSERT INTO dataset (
                workspace,
                name,
                version,
                log_message,
                timestamp,
                id_column,
                size,
                filename
            )
            VALUES (
                :workspace,
                :name,
                :version,
                :log_message,
                :timestamp,
                :id_column,
                :size,
                :filename
            )
            ;
            """),
            {
                'workspace': dataset_dict['workspace'],
                'name': dataset_dict['name'],
                'version': dataset_dict['version'],
                'log_message': dataset_dict['log_message'],
                'timestamp': dataset_dict['timestamp'],
                'id_column': dataset_dict['id_column'],
                'size': dataset_dict['size'],
                'filename': dataset_dict['filename']
            }
        )

        return cur.lastrowid


def create_one_dataset_tag(dataset_tag_dict):
    dataset_tag_dict['version_tag'] = int(dataset_tag_dict['version_tag'][1:])  # 'v1' -> 1

    with db_connection:
        cur = db_connection.cursor()
        cur.execute(
            dedent("""
            INSERT INTO dataset_tag (
                workspace,
                name,
                version,
                tag
            )
            VALUES (
                :workspace,
                :name,
                :version,
                :version_tag
            )
            ;
            """),
            dataset_tag_dict
        )

        return cur.lastrowid


def exist_dataset_by_version(workspace, name, version):
    with db_connection:
        cur = db_connection.cursor()
        cur = cur.execute(
            dedent("""
            SELECT EXISTS(
                SELECT 1
                FROM   dataset
                WHERE  workspace = :workspace
                AND    name = :name
                AND    version = :version
            )
            ;
            """),
            {
                'workspace': workspace,
                'name': name,
                'version': version,
            }
        )
        return cur.fetchone() is not None


def get_one_dataset_by_version(workspace, name, version='latest'):
    with db_connection:
        cur = db_connection.cursor()
        if version == 'none':
            cur.execute(
                dedent("""
                WITH base AS (
                    SELECT workspace,
                           name,
                           version,
                           log_message,
                           timestamp,
                           id_column,
                           size,
                           filename
                    FROM   dataset
                    WHERE  workspace = :workspace
                    AND    name = :name
                    AND    timestamp = (
                        SELECT MAX(timestamp)
                        FROM   dataset
                        WHERE  workspace = :workspace
                        AND    name = :name
                    )
                )
                ,tag AS (
                    SELECT  workspace,
                            name,
                            version,
                            tag
                    FROM    dataset_tag
                    WHERE   workspace = :workspace
                    AND     name = :name
                )
                SELECT  base.workspace,
                        base.name,
                        base.version,
                        tag.tag,
                        log_message,
                        timestamp,
                        id_column,
                        size,
                        filename
                FROM   base
                LEFT JOIN tag
                ON     base.workspace = tag.workspace
                AND    base.name = tag.name
                AND    base.version = tag.version
                ;
                """), {
                    'workspace': workspace,
                    'name': name,
                }
            )
        else:
            cur.execute(
                dedent("""
                WITH base AS (
                    SELECT workspace,
                           name,
                           version,
                           log_message,
                           timestamp,
                           id_column,
                           size,
                           filename
                    FROM   dataset
                    WHERE  workspace = :workspace
                    AND    name = :name
                    AND    version = :version
                )
                ,tag AS (
                    SELECT  workspace,
                            name,
                            version,
                            tag
                    FROM    dataset_tag
                    WHERE   workspace = :workspace
                    AND     name = :name
                    AND     version = :version
                )
                SELECT  base.workspace,
                        base.name,
                        base.version,
                        tag.tag,
                        log_message,
                        timestamp,
                        id_column,
                        size,
                        filename
                FROM   base
                LEFT JOIN tag
                ON     base.workspace = tag.workspace
                AND    base.name = tag.name
                AND    base.version = tag.version
                ;
                """), {
                    'workspace': workspace,
                    'name': name,
                    'version': version,
                }
            )
    po = cur.fetchone()
    return {
        'workspace': po[0],
        'name': po[1],
        'version': po[2],
        'version_tag': f'v{po[3]}' if po[3] is not None else None,
        'log_message': po[4],
        'timestamp': po[5],
        'id_column': po[6],
        'size': po[7],
        'filename': po[8],
    } if po is not None else None


def get_one_dataset_by_tag(workspace, name, tag):
    with db_connection:
        cur = db_connection.cursor()
        if tag == 'latest':
            cur.execute(dedent("""
            WITH base AS (
                SELECT workspace,
                       name,
                       version,
                       log_message,
                       timestamp,
                       id_column,
                       size,
                       filename
                FROM   dataset
                WHERE  workspace = :workspace
                AND    name = :name
            )
            ,tag AS (
                SELECT  workspace,
                        name,
                        version,
                        tag
                FROM    dataset_tag
                WHERE   workspace = :workspace
                AND     name = :name
                AND     tag = (
                    SELECT MAX(tag)
                    FROM   dataset_tag
                    WHERE  workspace = :workspace
                    AND    name = :name
                )
            )
            SELECT  base.workspace,
                    base.name,
                    base.version,
                    tag.tag,
                    log_message,
                    timestamp,
                    id_column,
                    size,
                    filename
            FROM   base
            JOIN tag
            ON     base.workspace = tag.workspace
            AND    base.name = tag.name
            AND    base.version = tag.version
            ;
            """))
        else:
            cur.execute(dedent("""
            WITH base AS (
                SELECT workspace,
                       name,
                       version,
                       log_message,
                       timestamp,
                       id_column,
                       size,
                       filename
                FROM   dataset
                WHERE  workspace = :workspace
                AND    name = :name
            )
            ,tag AS (
                SELECT  workspace,
                        name,
                        version,
                        tag
                FROM    dataset_tag
                WHERE   workspace = :workspace
                AND     name = :name
                AND     tag = :tag
            )
            SELECT  base.workspace,
                    base.name,
                    base.version,
                    tag.tag,
                    log_message,
                    timestamp,
                    id_column,
                    size,
                    filename
            FROM   base
            JOIN tag
            ON     base.workspace = tag.workspace
            AND    base.name = tag.name
            AND    base.version = tag.version
            ;
            """), {
                'workspace': workspace,
                'name': name,
                'tag': tag,
            })
        po = cur.fetchone()
        return {
            'workspace': po[0],
            'name': po[1],
            'version': po[2],
            'version_tag': f'v{po[3]}' if po[3] is not None else None,
            'log_message': po[4],
            'timestamp': po[5],
            'id_column': po[6],
            'size': po[7],
            'filename': po[8],
        } if po is not None else None


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
                            log_message,
                            timestamp,
                            id_column,
                            size,
                            filename
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
                       log_message,
                       timestamp,
                       id_column,
                       size,
                       filename
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
            'log_message': dataset_po[3],
            'timestamp': dataset_po[4],
            'id_column': dataset_po[5],
            'size': dataset_po[6],
            'filename': dataset_po[7],
        }
        dataset_dict_list.append(dataset_dict)
    return dataset_dict_list


def get_next_dataset_version_tag(workspace, name):
    with db_connection:
        cur = db_connection.cursor()
        cur.execute(
            """
            SELECT COALESCE(MAX(tag), 0) + 1
            FROM   dataset_tag
            WHERE  workspace = ?
            AND    name = ?
            ;
            """, (workspace, name,))
        return cur.fetchone()[0]
