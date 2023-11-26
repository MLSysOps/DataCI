#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Mar 14, 2023
"""
import sqlite3
from copy import deepcopy

from dataci.config import DB_FILE


def create_one_run(config: dict):
    config = deepcopy(config)
    job_config = config.pop('job')
    config['job_workspace'] = job_config['workspace']
    config['job_name'] = job_config['name']
    config['job_version'] = job_config['version']
    config['job_type'] = job_config['type']
    with sqlite3.connect(DB_FILE) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO run (
                workspace,
                name,
                version,
                status,
                job_workspace,
                job_name,
                job_version,
                job_type,
                create_time,
                update_time
            )
            VALUES (:workspace, :name, :version, :status, :job_workspace, :job_name, :job_version, :job_type, :create_time, :update_time)
            ;
            """,
            config
        )
        return cur.lastrowid


def update_one_run(config):
    config = deepcopy(config)
    job_config = config.pop('job')
    config['job_workspace'] = job_config['workspace']
    config['job_name'] = job_config['name']
    config['job_version'] = job_config['version']
    config['job_type'] = job_config['type']
    with sqlite3.connect(DB_FILE) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE run
            SET    status = :status
                 , job_workspace = :job_workspace
                 , job_name = :job_name
                 , job_version = :job_version
                 , job_type = :job_type
                 , update_time = :update_time
            WHERE  name = :name
            AND    version = :version
            ;
            """,
            config
        )
        return cur.lastrowid


def exist_run(name, version):
    with sqlite3.connect(DB_FILE) as conn:
        cur = conn.cursor()
        (exists,), = cur.execute(
            """
            SELECT EXISTS(
                SELECT 1
                FROM   run
                WHERE  name = ?
                AND    version = ?
            )
            ;
            """,
            (name, version)
        )
    return exists


def get_latest_run_version(name):
    with sqlite3.connect(DB_FILE) as conn:
        cur = conn.cursor()
        (version,), = cur.execute(
            """
            SELECT MAX(version)
            FROM   run
            WHERE  name = ?
            ;
            """,
            (name,)
        )
    return version or 0


def get_next_run_version(name):
    return get_latest_run_version(name) + 1


def get_one_run(name, version='latest'):
    with sqlite3.connect(DB_FILE) as conn:
        cur = conn.cursor()
        if version == 'latest':
            cur.execute(
                """
                SELECT workspace
                     , name
                     , version
                     , status
                     , job_workspace
                     , job_name
                     , job_version
                     , job_type
                     , create_time
                     , update_time
                FROM   run
                WHERE  name = ?
                ORDER BY version DESC
                LIMIT  1
                ;
                """,
                (name,)
            )
        else:
            cur.execute(
                """
                SELECT workspace
                     , name
                     , version
                     , status
                     , job_workspace
                     , job_name
                     , job_version
                     , job_type
                     , create_time
                     , update_time
                FROM   run
                WHERE  name = ?
                AND    version = ?
                ;
                """,
                (name, version)
            )

        config = cur.fetchone()
    return {
        'workspace': config[0],
        'name': config[1],
        'version': config[2],
        'status': config[3],
        'job': {
            'workspace': config[4],
            'name': config[5],
            'version': config[6],
            'type': config[7],
        },
        'create_time': config[8],
        'update_time': config[9],
    } if config else None
