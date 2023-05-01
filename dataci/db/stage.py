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
            INSERT INTO stage (workspace, name, version, script_path, cls_name, symbolize)
            VALUES (:workspace, :name, :version, :script_path, :cls_name, :symbolize)
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
            SET cls_name=:cls_name, symbolize=:symbolize
            WHERE workspace=:workspace AND name=:name AND version=:version
            """,
            stage_dict
        )
