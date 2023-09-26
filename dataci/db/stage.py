#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: May 01, 2023
"""
import json
import sqlite3
from textwrap import dedent

from dataci.config import DB_FILE


def create_one_stage(stage_dict):
    stage_dict = stage_dict.copy()
    stage_dict['params'] = json.dumps(stage_dict['params'])
    stage_dict['script_dir'] = stage_dict['script']['dir']
    stage_dict['script_entry'] = stage_dict['script']['entry']
    stage_dict['script_filelist'] = json.dumps(stage_dict['script']['filelist'], sort_keys=True)
    stage_dict['script_hash'] = stage_dict['script']['hash']

    with sqlite3.connect(DB_FILE) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO stage (
                workspace, name, version, params, timestamp, script_dir, script_entry, script_filelist, script_hash
            )
            VALUES (
                :workspace, :name, :version, :params, :timestamp
              , :script_dir, :script_entry, :script_filelist, :script_hash
            )
            ;
            """,
            stage_dict
        )


def create_one_stage_tag(stage_dict):
    stage_dict = stage_dict.copy()
    stage_dict['version_tag'] = int(stage_dict['version_tag'][1:])  # 'v1' -> 1

    with sqlite3.connect(DB_FILE) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO stage_tag (workspace, name, version, tag)
            VALUES (:workspace, :name, :version, :version_tag)
            ;
            """,
            stage_dict
        )


def exist_stage(workspace, name, version):
    with sqlite3.connect(DB_FILE) as conn:
        cur = conn.cursor()
        if version.startswith('v'):
            version = version[1:]
            cur.execute(
                dedent("""
                SELECT EXISTS(
                    SELECT 1
                    FROM   stage_tag
                    WHERE  workspace=:workspace
                    AND    name=:name
                    AND    tag=:version
                )
                """),
                {
                    'workspace': workspace,
                    'name': name,
                    'version': version
                }
            )
        else:
            cur.execute(
                dedent("""
                SELECT EXISTS(
                    SELECT 1 
                    FROM   stage 
                    WHERE  workspace=:workspace 
                    AND    name=:name 
                    AND    version=:version
                )
                """),
                {
                    'workspace': workspace,
                    'name': name,
                    'version': version
                }
            )
        return cur.fetchone()[0]


def get_one_stage_by_version(workspace, name, version='latest'):
    with sqlite3.connect(DB_FILE) as conn:
        cur = conn.cursor()
        # version is a hex string version
        if version == 'latest':
            cur.execute(
                dedent("""
                WITH base AS (
                    SELECT workspace, name, version, params, timestamp
                         , script_dir, script_entry, script_filelist, script_hash
                    FROM   stage
                    WHERE  workspace=:workspace
                    AND    name=:name
                    AND    timestamp = (
                        SELECT MAX(timestamp)
                        FROM   stage
                        WHERE  workspace=:workspace
                        AND    name=:name
                    )
                )
                ,tag AS (
                    SELECT workspace, name, version, tag
                    FROM   stage_tag
                    WHERE  workspace=:workspace
                    AND    name=:name
                )
                SELECT base.workspace
                     , base.name
                     , base.version
                     , tag
                     , params
                     , timestamp
                     , script_dir
                     , script_entry
                     , script_filelist
                     , script_hash
                FROM   base
                LEFT JOIN tag
                ON     base.workspace = tag.workspace
                AND    base.name = tag.name
                AND    base.version = tag.version
                ;
                """),
                {
                    'workspace': workspace,
                    'name': name,
                }
            )
        else:
            cur.execute(
                dedent("""
                WITH base AS (
                    SELECT workspace, name, version, params, timestamp
                         , script_dir, script_entry, script_filelist, script_hash
                    FROM   stage
                    WHERE  workspace=:workspace
                    AND    name=:name
                    AND    version=:version
                )
                ,tag AS (
                    SELECT workspace, name, version, tag
                    FROM   stage_tag
                    WHERE  workspace=:workspace
                    AND    name=:name
                    AND    version=:version
                )
                SELECT base.workspace
                     , base.name
                     , base.version
                     , tag
                     , params
                     , timestamp
                     , script_dir
                     , script_entry
                     , script_filelist
                     , script_hash
                FROM   base
                LEFT JOIN tag
                ON     base.workspace = tag.workspace
                AND    base.name = tag.name
                AND    base.version = tag.version
                ;
                """),
                {
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
        'version_tag': f'v{po[3]}' if po[3] else None,
        'params': json.loads(po[4]),
        'timestamp': po[5],
        'script': {
            'dir': po[6],
            'entry': po[7],
            'filelist': json.loads(po[8]),
            'hash': po[9],
        },
    } if po else None


def get_one_stage_by_tag(workspace, name, version_tag='latest'):
    with sqlite3.connect(DB_FILE) as conn:
        cur = conn.cursor()
        if version_tag == 'latest':
            cur.execute(
                dedent("""
                WITH base AS (
                    SELECT workspace, name, version, params, timestamp
                         , script_dir, script_entry, script_filelist, script_hash
                    FROM   stage
                    WHERE  workspace=:workspace
                    AND    name=:name
                )
                ,tag AS (
                    SELECT workspace, name, version, tag
                    FROM   stage_tag
                    WHERE  workspace=:workspace
                    AND    name=:name
                    AND    tag = (
                        SELECT MAX(tag)
                        FROM   stage_tag
                        WHERE  workspace=:workspace
                        AND    name=:name
                    )
                )
                SELECT base.workspace
                        , base.name
                        , base.version
                        , tag
                        , params
                        , timestamp
                        , script_dir
                        , script_entry
                        , script_filelist
                        , script_hash
                FROM   base
                JOIN   tag
                ON     base.workspace = tag.workspace
                AND    base.name = tag.name
                AND    base.version = tag.version
                ;
                """),
                {
                    'workspace': workspace,
                    'name': name,
                }
            )
        else:
            version_tag = version_tag[1:]
            cur.execute(
                dedent("""
                WITH base AS (
                    SELECT workspace, name, version, params, timestamp
                         , script_dir, script_entry, script_filelist, script_hash
                    FROM   stage
                    WHERE  workspace=:workspace
                    AND    name=:name
                )
                ,tag AS (
                    SELECT workspace, name, version, tag
                    FROM   stage_tag
                    WHERE  workspace=:workspace
                    AND    name=:name
                    AND    tag=:version_tag
                )
                SELECT base.workspace
                        , base.name
                        , base.version
                        , tag
                        , params
                        , timestamp
                        , script_dir
                        , script_entry
                        , script_filelist
                        , script_hash
                FROM   base
                JOIN   tag
                ON     base.workspace = tag.workspace
                AND    base.name = tag.name
                AND    base.version = tag.version
                ;
                """),
                {
                    'workspace': workspace,
                    'name': name,
                    'version_tag': version_tag,
                }
            )

        po = cur.fetchone()

    return {
        'workspace': po[0],
        'name': po[1],
        'version': po[2],
        'version_tag': f'v{po[3]}',
        'params': json.loads(po[4]),
        'timestamp': po[5],
        'script': {
            'dir': po[6],
            'entry': po[7],
            'filelist': json.loads(po[8]),
            'hash': po[9],
        },
    } if po else None


def get_many_stages(workspace, name, version=None, all=False):
    # replace None with '', since None will lead to issues in SQL
    version = version or ''
    with sqlite3.connect(DB_FILE) as conn:
        cur = conn.cursor()
        if version == '':
            # Get the head version
            cur.execute(
                """
                SELECT workspace, name, version, params, script_path, timestamp, symbolize
                FROM   stage 
                WHERE  workspace=:workspace 
                AND    name=:name
                AND    version =:version
                """,
                {
                    'workspace': workspace,
                    'name': name,
                    'version': version,
                }
            )
            po = cur.fetchall()
            if po is None:
                # If there is no head version, get the latest version (by set version to None)
                version = 'latest'
        if version == 'latest':
            # Get the latest version
            cur.execute(
                """
                SELECT workspace, name, version, params, script_path, timestamp, symbolize
                FROM   (
                    SELECT *
                           ,ROW_NUMBER() OVER (PARTITION BY workspace, name ORDER BY version DESC) AS _rn
                    FROM   (
                        SELECT workspace, name, version, params, script_path, timestamp, symbolize
                        FROM   stage 
                        WHERE  workspace=:workspace 
                        AND    name GLOB :name
                        AND    version <> ''
                    )
                )
                WHERE _rn = 1
                ;
                """,
                {
                    'workspace': workspace,
                    'name': name,
                }
            )
            po = cur.fetchall()
        elif version != '':
            cur.execute(
                f"""
                SELECT workspace, name, version, params, script_path, timestamp, symbolize
                FROM   stage 
                WHERE  workspace=:workspace 
                AND    name GLOB :name 
                AND    version GLOB :version
                {"AND    version <> ''" if not all else ''}
                """,
                {
                    'workspace': workspace,
                    'name': name,
                    'version': version,
                }
            )
            po = cur.fetchall()

    return [
        {
            'workspace': p[0],
            'name': p[1],
            'version': p[2],
            'params': json.loads(p[3]),
            'script_path': p[4],
            'timestamp': p[5],
            'symbolize': p[6],
        }
        for p in po
    ]


def get_next_stage_version_tag(workspace, name):
    with sqlite3.connect(DB_FILE) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT COALESCE(MAX(tag), 0) + 1
            FROM   stage_tag
            WHERE  workspace=:workspace 
            AND    name=:name
            """,
            {
                'workspace': workspace,
                'name': name,
            }
        )
        return 'v' + str(cur.fetchone()[0])
