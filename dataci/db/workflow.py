#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Mar 13, 2023
"""
import json
import sqlite3
from copy import deepcopy
from textwrap import dedent

from dataci.config import DB_FILE


def create_one_workflow(config):
    """Create one workflow."""
    workflow_dict = deepcopy(config)
    workflow_dict['trigger'] = json.dumps(workflow_dict['trigger'], sort_keys=True)
    dag = workflow_dict.pop('dag')
    workflow_dict['dag'] = json.dumps(dag['edge'], sort_keys=True)
    workflow_dag_node_dict = dag['node']

    with sqlite3.connect(DB_FILE).cursor() as cur:
        cur.execute(
            """
            INSERT INTO workflow (workspace, name, version, timestamp, schedule, dag)
            VALUES (:workspace, :name, :version, :timestamp, :trigger, :dag)
            """,
            workflow_dict,
        )
        workflow_id = cur.lastrowid
        cur.executemany(
            """
            INSERT INTO workflow_dag_node ( workflow_workspace
                                          , workflow_name
                                          , workflow_version
                                          , stage_workspace
                                          , stage_name
                                          , stage_version
                                          , dag_node_id)
            VALUES ( :workflow_workspace
                   , :workflow_name
                   , :workflow_version
                   , :stage_workspace
                   , :stage_name
                   , :stage_version
                   , :dag_node_id);
            """,
            [
                {
                    'workflow_workspace': workflow_dict['workspace'],
                    'workflow_name': workflow_dict['name'],
                    'workflow_version': workflow_dict['version'],
                    'stage_workspace': node['workspace'],
                    'stage_name': node['name'],
                    'stage_version': node['version'] or '',
                    'dag_node_id': node_id,
                }
                for node_id, node in workflow_dag_node_dict.items()
            ],
        )
        return workflow_id


def create_one_workflow_tag(config):
    """Create one workflow tag."""
    workflow_tag_dict = deepcopy(config)
    workflow_tag_dict['version_tag'] = int(workflow_tag_dict['version_tag'][1:])  # remove 'v' prefix

    with sqlite3.connect(DB_FILE).cursor() as cur:
        cur.execute(
            """
            INSERT INTO workflow_tag (workspace, name, version, tag)
            VALUES (:workspace, :name, :version, :version_tag)
            """,
            workflow_tag_dict,
        )
        return cur.lastrowid


def exist_workflow_by_version(workspace, name, version):
    """Check if the workflow exists."""
    with sqlite3.connect(DB_FILE).cursor() as cur:
        cur.execute(
            dedent(f"""
                    SELECT EXISTS(
                        SELECT 1
                        FROM   workflow
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


def exist_workflow_by_tag(workspace, name, tag):
    """Check if the workflow exists."""
    tag = int(tag[1:])  # remove 'v' prefix
    with sqlite3.connect(DB_FILE).cursor() as cur:
        cur.execute(
            dedent("""
                    SELECT EXISTS(
                        SELECT 1
                        FROM   workflow_tag
                        WHERE  workspace=:workspace
                        AND    name=:name
                        AND    tag=:tag
                    )
                    """),
            {
                'workspace': workspace,
                'name': name,
                'tag': tag
            }
        )
        return cur.fetchone()[0]


def get_one_workflow_by_version(workspace, name, version):
    with sqlite3.connect(DB_FILE).cursor() as cur:
        if version is None:
            # Get the latest version
            cur.execute(
                dedent("""
                WITH base AS (
                    SELECT *
                    FROM   workflow
                    WHERE  workspace = :workspace 
                    AND    name = :name
                )
                ,latest AS (
                    SELECT MAX(timestamp) AS timestamp
                    FROM   base
                )
                ,tag AS (
                    SELECT version, tag
                    FROM   workflow_tag
                    WHERE  workspace = :workspace
                    AND    name = :name
                )
                SELECT workspace, name, base.version, tag, base.timestamp, params, flag, schedule, dag
                FROM   base
                JOIN   latest
                ON     base.timestamp = latest.timestamp
                LEFT JOIN tag
                ON     base.version = tag.version
                ;
                """),
                {
                    'workspace': workspace,
                    'name': name,
                }
            )
        else:
            # Get the specified version
            cur.execute(
                dedent("""
                WITH base AS (
                    SELECT workspace, name, version, timestamp, params, flag, schedule, dag
                    FROM   workflow
                    WHERE  workspace = :workspace
                    AND    name = :name
                    AND    version = :version
                )
                ,tag AS (
                    SELECT version, tag
                    FROM   workflow_tag
                    WHERE  workspace = :workspace
                    AND    name = :name
                    AND    version = :version
                )
                SELECT workspace, name, base.version, tag, timestamp, params, flag, schedule, dag
                FROM   base
                LEFT JOIN tag
                ON     base.version = tag.version
                ;
                """),
                {
                    'workspace': workspace,
                    'name': name,
                    'version': version,
                }
            )

        workflow = cur.fetchone()
        if workflow is None:
            return None
        workflow_dict = {
            'workspace': workflow[0],
            'name': workflow[1],
            'version': workflow[2],
            'version_tag': f'v{workflow[3]}' if workflow[3] else None,
            'timestamp': workflow[4],
            'params': '',
            'flag': '',
            'trigger': json.loads(workflow[7]),
            'dag': {
                'edge': json.loads(workflow[8]),
            }
        }
        # Overwrite the query version for dag node
        version = workflow_dict['version']
        cur.execute(
            f"""
            SELECT stage_workspace, stage_name, stage_version, dag_node_id
            FROM   workflow_dag_node
            WHERE  workflow_workspace=:workspace
            AND    workflow_name=:name
            AND    workflow_version =:version
            ;
            """,
            {
                'workspace': workspace,
                'name': name,
                'version': version,
            },
        )
        workflow_dict['dag']['node'] = {
            node[3]: {
                'workspace': node[0],
                'name': node[1],
                'version': node[2] if node[2] != '' else None,
            } for node in cur.fetchall()
        }
        return workflow_dict


def get_one_workflow_by_tag(workspace, name, tag):
    with sqlite3.connect(DB_FILE).cursor() as cur:
        if tag is None or tag == 'latest':
            cur.execute(
                dedent("""
                WITH tag AS (
                    SELECT version, tag
                    FROM   workflow_tag
                    WHERE  workspace = :workspace
                    AND    name = :name
                )
                ,latest AS (
                    SELECT MAX(tag) AS tag
                    FROM   tag
                )
                ,base AS (
                    SELECT workspace, name, version, timestamp, params, flag, schedule, dag
                    FROM   workflow
                    WHERE  workspace = :workspace
                    AND    name = :name
                )
                SELECT workspace, name, tag.version, tag.tag, timestamp, params, flag, schedule, dag
                FROM   tag
                JOIN   latest
                ON     tag.tag = latest.tag
                JOIN   base
                ON     tag.version = base.version
                ;
                """),
                {
                    'workspace': workspace,
                    'name': name,
                }
            )
        else:
            tag = int(tag[1:])  # remove 'v' prefix
            cur.execute(
                dedent("""
                WITH base AS (
                    SELECT workspace, name, version, timestamp, params, flag, schedule, dag
                    FROM   workflow
                    WHERE  workspace = :workspace
                    AND    name = :name
                )
                ,tag AS (
                    SELECT version, tag
                    FROM   workflow_tag
                    WHERE  workspace = :workspace
                    AND    name = :name
                    AND    tag = :tag
                )
                SELECT workspace, name, base.version, tag, timestamp, params, flag, schedule, dag
                FROM   base
                JOIN   tag
                ON     base.version = tag.version
                ;
                """),
                {
                    'workspace': workspace,
                    'name': name,
                    'tag': tag,
                }
            )
        config = cur.fetchone()
        workflow_dict = {
            'workspace': config[0],
            'name': config[1],
            'version': config[2],
            'version_tag': f'v{config[3]}',
            'timestamp': config[4],
            'params': '',
            'flag': '',
            'trigger': json.loads(config[6]),
            'dag': {
                'edge': json.loads(config[8]),
            }
        }
        # Overwrite the query version for dag node
        version = workflow_dict['version']
        cur.execute(
            dedent("""
            SELECT stage_workspace, stage_name, stage_version, dag_node_id
            FROM   workflow_dag_node
            WHERE  workflow_workspace=:workspace
            AND    workflow_name=:name
            AND    workflow_version =:version
            ;
            """),
            {
                'workspace': workspace,
                'name': name,
                'version': version,
            },
        )
        workflow_dict['dag']['node'] = {
            node[3]: {
                'workspace': node[0],
                'name': node[1],
                'version': node[2] if node[2] != '' else None,
            } for node in cur.fetchall()
        }
        return workflow_dict


def get_many_workflow(workspace, name, version=None):
    # replace None with '', since None will lead to issues in SQL
    version = version or ''
    with sqlite3.connect(DB_FILE).cursor() as cur:
        if version == '':
            # Get the head version
            cur.execute(
                """
                SELECT workspace, name, version, timestamp, params, flag, schedule, dag
                FROM   workflow 
                WHERE  workspace = :workspace 
                AND    name GLOB :name
                AND    version = :version
                ;
                """,
                {
                    'workspace': workspace,
                    'name': name,
                    'version': version,
                }
            )
            workflow_po_iter = cur.fetchall()
            if workflow_po_iter is None:
                # If there is no head version, get the latest version (by set version to None)
                version = 'latest'
        if version == 'latest':
            # Get the latest version
            cur.execute(
                """
                SELECT workspace, name, version, timestamp, params, flag, schedule, dag
                FROM (
                    SELECT workspace
                         , name
                         , version
                         , timestamp
                         , params
                         , flag
                         , schedule
                         , dag
                         , row_number() OVER (PARTITION BY workspace, name ORDER BY version DESC) AS rk
                    FROM (
                        SELECT * FROM workflow
                        WHERE  workspace=:workspace
                        AND    name GLOB :name
                        AND    length(version) < 32
                    )
                )
                WHERE rk = 1
                ;
                """,
                {
                    'workspace': workspace,
                    'name': name,
                },
            )
            workflow_po_iter = cur.fetchall()
        elif version != '':
            cur.execute(
                """
                SELECT workspace, name, version, timestamp, params, flag, schedule, dag
                FROM   workflow
                WHERE  workspace =:workspace 
                AND    name GLOB :name 
                AND    version GLOB :version
                """,
                {
                    'workspace': workspace,
                    'name': name,
                    'version': version,
                },
            )
            workflow_po_iter = cur.fetchall()
        workflow_list = [
            {
                'workspace': workflow[0],
                'name': workflow[1],
                'version': workflow[2],
                'timestamp': workflow[3],
                'params': json.loads(workflow[4]),
                'flag': json.loads(workflow[5]),
                'schedule': workflow[6].split(',') if workflow[6] != '' else list(),  # schedule is a list
                'dag': {
                    'edge': json.loads(workflow[7]),
                }
            } for workflow in workflow_po_iter
        ]
        # Query dag nodes for each workflow
        for workflow in workflow_list:
            cur.execute(
                f"""
                SELECT stage_workspace, stage_name, stage_version, dag_node_id
                FROM   workflow_dag_node
                WHERE  workflow_workspace=:workspace
                AND    workflow_name=:name
                AND    workflow_version =:version
                """,
                workflow,
            )
            workflow['version'] = workflow['version'] if workflow['version'] != '' else None
            workflow['dag']['node'] = {
                node[3]: {
                    'workspace': node[0],
                    'name': node[1],
                    'version': node[2] if node[2] != '' else None,
                } for node in cur.fetchall()
            }
        return workflow_list


def get_all_workflow_schedule(latest_only=True):
    with sqlite3.connect(DB_FILE).cursor() as cur:
        cur.execute(
            f"""
            WITH base AS (
                SELECT workspace, name, version, schedule
                FROM   (
                    SELECT workspace
                         , name
                         , version
                         , schedule
                    FROM   workflow
                )
            )
            , tag AS (
                SELECT *
                FROM (
                    SELECT workspace
                         , name
                         , version
                         , tag
                         , ROW_NUMBER() OVER (PARTITION BY workspace, name ORDER BY tag DESC) AS rk
                    FROM   workflow_tag
                )
                {"WHERE  rk = 1" if latest_only else ''}
            )
            SELECT base.workspace
                 , base.name
                 , base.version
                 , tag.tag
                 , base.schedule
            FROM   base
            JOIN   tag
            ON     base.workspace = tag.workspace
            AND    base.name = tag.name
            ;
            """
        )
        return [
            {
                'workspace': workflow[0],
                'name': workflow[1],
                'version': workflow[2] if workflow[2] != '' else None,
                'version_tag': f'v{workflow[3]}' if workflow[3] else None,
                'trigger': json.loads(workflow[4]),
            } for workflow in cur.fetchall()
        ]


def get_next_workflow_version_id(workspace, name):
    with sqlite3.connect(DB_FILE).cursor() as cur:
        cur.execute(
            """
            SELECT COALESCE(MAX(CAST(tag AS INTEGER)), 0) + 1
            FROM   workflow_tag
            WHERE  workspace=:workspace 
            AND    name=:name
            ;
            """,
            {
                'workspace': workspace,
                'name': name,
            },
        )
        return 'v' + str(cur.fetchone()[0])
