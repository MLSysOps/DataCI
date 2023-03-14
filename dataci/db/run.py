#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Mar 14, 2023
"""
from . import db_connection


def get_next_run_num(pipeline_name, pipeline_version):
    with db_connection:
        (next_run_id,), = db_connection.execute(
            """
            SELECT COALESCE(MAX(run_num), 0) + 1 AS next_run_id 
            FROM run
            WHERE pipeline_name = ?
            AND   pipeline_version = ?
            """,
            (pipeline_name, pipeline_version)
        )
    return next_run_id
