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
            ;
            """,
            (pipeline_name, pipeline_version)
        )
    return next_run_id


def create_one_run(run_dict):
    pipeline_dict = run_dict['pipeline']
    with db_connection:
        db_connection.execute(
            """
            INSERT INTO run(run_num, pipeline_name, pipeline_version) VALUES
            (?,?,?)
            ;
            """,
            (run_dict['run_num'], pipeline_dict['name'],
             pipeline_dict['version'])
        )


def get_many_runs(pipeline_name, pipeline_version):
    with db_connection:
        run_dict_iter = db_connection.execute(
            """
            SELECT run.*,
                   timestamp
            FROM (
                SELECT run_num,
                    pipeline_name,
                    pipeline_version
                FROM   run
                WHERE  pipeline_name GLOB ?
                AND    pipeline_version GLOB ?
            ) run
            JOIN (
                SELECT name,
                       version,
                       timestamp
                FROM   pipeline
                WHERE  name GLOB ?
                AND    version GLOB ?
            ) pipeline
            ON pipeline_name = name
            AND pipeline_version = version
            ;
            """,
            (pipeline_name, pipeline_version, pipeline_name, pipeline_version),
        )
    run_dict_list = list()
    for run_po in run_dict_iter:
        run_num, pipeline_name, pipeline_version, timestamp = run_po
        run_dict_list.append({
            'run_num': run_num,
            'pipeline': {'name': pipeline_name, 'version': pipeline_version, 'timestamp': timestamp},
        })
    return run_dict_list
