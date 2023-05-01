#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: May 01, 2023
"""
from . import db_connection


def create_one_stage(stage_dict):
    with db_connection:
        cur = db_connection.cursor()
        cur.execute(
            """
            INSERT INTO stage (workspace, name, version, script_path, timestamp, symbolize)
            VALUES (:workspace, :name, :version, :script_path, :timestamp, :symbolize)
            """,
            stage_dict
        )


def exist_stage(workspace, name, version):
    with db_connection:
        cur = db_connection.cursor()
        cur.execute(
            """
            SELECT EXISTS(
                SELECT 1 
                FROM   stage 
                WHERE  workspace=:workspace 
                AND    name=:name 
                AND    version=:version
            )
            """,
            {
                'workspace': workspace,
                'name': name,
                'version': version
            }
        )
        return cur.fetchone()[0]


def update_one_stage(stage_dict):
    with db_connection:
        cur = db_connection.cursor()
        cur.execute(
            """
            UPDATE stage
            SET timestamp=:timestamp, symbolize=:symbolize
            WHERE workspace=:workspace AND name=:name AND version=:version
            """,
            stage_dict
        )


def get_one_stage(workspace, name, version=None):
    # Version is None, get the latest version
    # Some illustration of versions:
    # v1 -> v2 (latest) -> (head)
    # In this case,
    #   version=latest: get v2
    #   version=head: get head
    #   version=v1/v2: get v1/v2
    #
    # v1 -> v2 (latest, head)
    # In this case,
    #   version=latest: get v2
    #   version=head: get v2 (there will be no head in DB)
    #   version=v1/v2: get v1/v2
    with db_connection:
        cur = db_connection.cursor()
        if version == 'head':
            # Get the head version
            cur.execute(
                """
                SELECT workspace, name, version, script_path, timestamp, symbolize
                FROM   stage 
                WHERE  workspace=:workspace 
                AND    name=:name
                AND    version = 'head'
                """,
                {
                    'workspace': workspace,
                    'name': name,
                }
            )
            po = cur.fetchone()
            if po is None:
                # If there is no head version, get the latest version (by set version to None)
                version = None
        if version is None or version == 'latest':
            # Get the latest version
            cur.execute(
                """
                SELECT workspace, name, version, script_path, timestamp, symbolize
                FROM   stage 
                WHERE  workspace=:workspace 
                AND    name=:name
                AND    version <> 'head'
                ORDER BY version DESC
                LIMIT 1
                """,
                {
                    'workspace': workspace,
                    'name': name,
                }
            )
            po = cur.fetchone()
        elif version != 'head':
            cur.execute(
                """
                SELECT workspace, name, version, script_path, timestamp, symbolize
                FROM   stage 
                WHERE  workspace=:workspace 
                AND    name=:name 
                AND    version=:version
                """,
                {
                    'workspace': workspace,
                    'name': name,
                    'version': version,
                }
            )
            po = cur.fetchone()

    return dict(zip(['workspace', 'name', 'version', 'script_path', 'timestamp', 'symbolize'], po))
