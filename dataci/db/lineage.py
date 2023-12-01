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


def exist_one_lineage(upstream_config, downstream_config):
    with sqlite3.connect(DB_FILE) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT EXISTS(
                SELECT 1
                FROM lineage
                WHERE upstream_workspace = :upstream_workspace
                    AND upstream_name = :upstream_name
                    AND upstream_version = :upstream_version
                    AND upstream_type = :upstream_type
                    AND downstream_workspace = :downstream_workspace
                    AND downstream_name = :downstream_name
                    AND downstream_version = :downstream_version
                    AND downstream_type = :downstream_type
                  )
            ;
            """,
            {
                'upstream_workspace': upstream_config['workspace'],
                'upstream_name': upstream_config['name'],
                'upstream_version': upstream_config['version'],
                'upstream_type': upstream_config['type'],
                'downstream_workspace': downstream_config['workspace'],
                'downstream_name': downstream_config['name'],
                'downstream_version': downstream_config['version'],
                'downstream_type': downstream_config['type'],
            }
        )
        return cur.fetchone()[0]


def exist_many_downstream_lineage(upstream_config, downstream_configs):
    # Return empty list if no upstream_configs or downstream_configs,
    # this prevents SQL syntax error when generating SQL statement
    if len(downstream_configs) == 0:
        return list()

    with sqlite3.connect(DB_FILE) as conn:
        cur = conn.cursor()
        sql_lineage_values = ',\n'.join([
                repr((
                    downstream_config['workspace'],
                    downstream_config['name'],
                    downstream_config['version'],
                    downstream_config['type'],
                ))
                for downstream_config in downstream_configs
        ])
        cur.execute(
            f"""
            WITH downstreams (
                downstream_workspace
                ,downstream_name
                ,downstream_version
                ,downstream_type
            ) AS (
                VALUES {sql_lineage_values}
            )
            ,lineages AS (
                SELECT TRUE AS flg
                       ,upstream_workspace
                       ,upstream_name
                       ,upstream_version
                       ,upstream_type
                       ,downstream_workspace
                       ,downstream_name
                       ,downstream_version
                       ,downstream_type
                FROM lineage
                WHERE upstream_workspace = :upstream_workspace
                    AND upstream_name = :upstream_name
                    AND upstream_version = :upstream_version
                    AND upstream_type = :upstream_type
            )
            SELECT COALESCE(flg, FALSE) AS flg
            FROM downstreams
            LEFT JOIN lineages USING (
                downstream_workspace
                ,downstream_name
                ,downstream_version
                ,downstream_type
            )
            ;
            """,
            {
                'upstream_workspace': upstream_config['workspace'],
                'upstream_name': upstream_config['name'],
                'upstream_version': upstream_config['version'],
                'upstream_type': upstream_config['type'],
            }
        )
        return [bool(row[0]) for row in cur.fetchall()]


def exist_many_upstream_lineage(upstream_configs, downstream_config):
    # Return empty list if no upstream_configs or downstream_configs,
    # this prevents SQL syntax error when generating SQL statement
    if len(upstream_configs) == 0:
        return list()

    with sqlite3.connect(DB_FILE) as conn:
        cur = conn.cursor()
        sql_lineage_values = ',\n'.join([
                repr((
                    upstream_config['workspace'],
                    upstream_config['name'],
                    upstream_config['version'],
                    upstream_config['type'],
                ))
                for upstream_config in upstream_configs
        ])
        cur.execute(
            f"""
            WITH upstreams (
                upstream_workspace
                ,upstream_name
                ,upstream_version
                ,upstream_type
            ) AS (
                VALUES {sql_lineage_values}
            )
            ,lineages AS (
                SELECT TRUE AS flg
                       ,upstream_workspace
                       ,upstream_name
                       ,upstream_version
                       ,upstream_type
                       ,downstream_workspace
                       ,downstream_name
                       ,downstream_version
                       ,downstream_type
                FROM lineage
                WHERE downstream_workspace = :downstream_workspace
                    AND downstream_name = :downstream_name
                    AND downstream_version = :downstream_version
                    AND downstream_type = :downstream_type
            )
            SELECT COALESCE(flg, FALSE) AS flg
            FROM upstreams
            LEFT JOIN lineages USING (
                upstream_workspace
                ,upstream_name
                ,upstream_version
                ,upstream_type
            )
            ;
            """,
            {
                'downstream_workspace': downstream_config['workspace'],
                'downstream_name': downstream_config['name'],
                'downstream_version': downstream_config['version'],
                'downstream_type': downstream_config['type'],
            }
        )
        return [bool(row[0]) for row in cur.fetchall()]


def create_many_lineage(config):
    # Permute all upstream and downstream lineage
    lineage_configs = list()
    for upstream_config in config['upstream']:
        for downstream_config in config['downstream']:
            lineage_configs.append({
                'upstream_workspace': upstream_config['workspace'],
                'upstream_name': upstream_config['name'],
                'upstream_version': upstream_config['version'],
                'upstream_type': upstream_config['type'],
                'downstream_workspace': downstream_config['workspace'],
                'downstream_name': downstream_config['name'],
                'downstream_version': downstream_config['version'],
                'downstream_type': downstream_config['type'],
            })

    with sqlite3.connect(DB_FILE) as conn:
        cur = conn.cursor()
        cur.executemany(
            """
            INSERT INTO lineage (
                upstream_workspace
                ,upstream_name
                ,upstream_version
                ,upstream_type
                ,downstream_workspace
                ,downstream_name
                ,downstream_version
                ,downstream_type
            )
            VALUES (
                :upstream_workspace
                ,:upstream_name
                ,:upstream_version
                ,:upstream_type
                ,:downstream_workspace
                ,:downstream_name
                ,:downstream_version
                ,:downstream_type
            )
            ;
            """,
            lineage_configs,
        )
