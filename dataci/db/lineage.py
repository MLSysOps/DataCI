#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Nov 27, 2023
"""
import sqlite3

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

    run_dataset_params = [
        {
            'run_name': run_name,
            'run_version': run_version,
            'dataset_workspace': dataset_configs['workspace'],
            'dataset_name': dataset_configs['name'],
            'dataset_version': dataset_configs['version'],
            'direction': dataset_configs['direction'],
        }
        for dataset_configs in dataset_configs
    ]

    with sqlite3.connect(DB_FILE) as conn:
        cur = conn.cursor()
        cur.executemany(
            """
            SELECT EXISTS(
                SELECT 1
                FROM run_dataset_lineage
                WHERE run_name = :run_name
                  AND run_version = :run_version
                  AND dataset_workspace = :dataset_workspace
                  AND dataset_name = :dataset_name
                  AND dataset_version = :dataset_version
                  AND direction = :direction
                  )
            ;
            """,
            run_dataset_params,
        )
        return [row[0] for row in cur.fetchall()]


def create_one_lineage(config):
    run_lineage_config = {
        'run_name': config['run']['name'],
        'run_version': config['run']['version'],
        'parent_run_name': config['parent_run']['name'],
        'parent_run_version': config['parent_run']['version'],
    }

    run_dataset_lineage_configs = list()
    for dataset in config['inputs']:
        run_dataset_lineage_configs.append({
            'run_name': config['run']['name'],
            'run_version': config['run']['version'],
            'dataset_name': dataset['name'],
            'dataset_version': dataset['version'],
            'direction': 'input',
        })
    for dataset in config['outputs']:
        run_dataset_lineage_configs.append({
            'run_name': config['run']['name'],
            'run_version': config['run']['version'],
            'dataset_name': dataset['name'],
            'dataset_version': dataset['version'],
            'direction': 'output',
        })

    with sqlite3.connect(DB_FILE) as conn:
        cur = conn.cursor()
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

        # add run -> dataset
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
            run_dataset_lineage_configs
        )
