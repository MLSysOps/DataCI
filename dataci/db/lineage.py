#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Nov 27, 2023
"""
import sqlite3
from contextlib import nullcontext

from dataci.config import DB_FILE


def get_lineage():
    pass


def exist_run_lineage(run_name, run_version, parent_run_name, parent_run_version):
    with sqlite3.connect(DB_FILE) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT EXISTS(
                SELECT 1
                FROM run_lineage
                WHERE run_name = :run_name
                  AND run_version = :run_version
                  AND parent_run_name = :parent_run_name
                  AND parent_run_version = :parent_run_version
                  )
            ;
            """,
            {
                'run_name': run_name,
                'run_version': run_version,
                'parent_run_name': parent_run_name,
                'parent_run_version': parent_run_version,
            }
        )
        return cur.fetchone()[0]


def exist_many_run_dataset_lineage(run_name, run_version, dataset_configs):
    # Return empty list if no dataset_configs,
    # this prevents SQL syntax error when generating SQL statement
    if len(dataset_configs) == 0:
        return list()

    with sqlite3.connect(DB_FILE) as conn:
        cur = conn.cursor()
        sql_dataset_values = ',\n'.join([
                repr((dataset_config['workspace'], dataset_config['name'], dataset_config['version'],
                      dataset_config['direction']))
                for dataset_config in dataset_configs
        ])
        cur.execute(
            f"""
            WITH datasets (dataset_workspace, dataset_name, dataset_version, direction) AS (
                VALUES {sql_dataset_values}
            )
            ,lineage AS (
                SELECT TRUE AS flg
                       ,dataset_workspace
                       ,dataset_name
                       ,dataset_version
                       ,direction
                FROM run_dataset_lineage
                WHERE run_name = :run_name
                  AND run_version = :run_version
            )
            SELECT COALESCE(flg, FALSE) AS flg
            FROM datasets
            LEFT JOIN lineage USING (dataset_workspace, dataset_name, dataset_version, direction)
            ;
            """,
            {
                'run_name': run_name,
                'run_version': run_version,
            }
        )
        return [bool(row[0]) for row in cur.fetchall()]


def create_one_run_lineage(config, cursor=None):
    run_lineage_config = {
        'run_name': config['run']['name'],
        'run_version': config['run']['version'],
        'parent_run_name': config['parent_run']['name'],
        'parent_run_version': config['parent_run']['version'],
    }

    with sqlite3.connect(DB_FILE) if cursor is None else nullcontext() as conn:
        cur = cursor or conn.cursor()
        # add parent_run -> run
        cur.execute(
            """
            INSERT INTO run_lineage (
                 run_name
                 ,run_version
                 ,parent_run_name
                 ,parent_run_version
            )
            VALUES (:run_name, :run_version, :parent_run_name, :parent_run_version)
            ;
            """,
            run_lineage_config
        )


def create_many_dataset_lineage(config, cursor=None):
    dataset_configs = list()
    for dataset in config['inputs'] + config['outputs']:
        dataset_configs.append({
            'run_name': config['run']['name'],
            'run_version': config['run']['version'],
            'dataset_workspace': dataset['workspace'],
            'dataset_name': dataset['name'],
            'dataset_version': dataset['version'],
            'direction': dataset['direction'],
        })

    with sqlite3.connect(DB_FILE) if cursor is None else nullcontext() as conn:
        cur = cursor or conn.cursor()
        cur.executemany(
            """
            INSERT INTO run_dataset_lineage (
                run_name
                ,run_version
                ,dataset_workspace
                ,dataset_name
                ,dataset_version
                ,direction
            )
            VALUES (:run_name, :run_version, :dataset_workspace, :dataset_name, :dataset_version, :direction)
            ;
            """,
            dataset_configs,
        )
