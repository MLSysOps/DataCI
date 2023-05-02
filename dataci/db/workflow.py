#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Mar 13, 2023
"""
import json

from . import db_connection


def create_one_workflow(workflow_dict):
    """Create one workflow."""
    dag = workflow_dict.pop('dag')
    workflow_dict['params'] = json.dumps(workflow_dict['params'], sort_keys=True)
    workflow_dict['flag'] = json.dumps(workflow_dict['flag'], sort_keys=True)
    workflow_dict['dag'] = json.dumps(dag['edge'], sort_keys=True)
    workflow_dag_node_dict = dag['node']
    workflow_dict['version'] = workflow_dict['version'] or ''

    with db_connection:
        cur = db_connection.cursor()
        cur.execute(
            """
            INSERT INTO workflow (workspace, name, version, timestamp, params, flag, dag)
            VALUES (:workspace, :name, :version, :timestamp, :params, :flag, :dag)
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


def exist_workflow(workspace, name, version):
    """Check if the workflow exists."""
    version = version or ''
    with db_connection:
        cur = db_connection.cursor()
        cur.execute(
            f"""
            SELECT EXISTS(
                SELECT 1 
                FROM   workflow 
                WHERE  workspace=:workspace 
                AND    name=:name 
                AND    version=:version
            )
            """,
            {
                'workspace': workspace,
                'name': name,
                'version': version,
            },
        )
        return cur.fetchone()[0]


def update_one_workflow(workflow_dict):
    dag = workflow_dict.pop('dag')
    workflow_dict['params'] = json.dumps(workflow_dict['params'], sort_keys=True)
    workflow_dict['flag'] = json.dumps(workflow_dict['flag'], sort_keys=True)
    workflow_dict['dag'] = json.dumps(dag['edge'], sort_keys=True)
    workflow_dag_node_dict = dag['node']
    workflow_dict['version'] = workflow_dict['version'] or ''

    with db_connection:
        cur = db_connection.cursor()
        cur.execute(
            """
            UPDATE workflow
            SET timestamp=:timestamp, params=:params, flag=:flag, dag=:dag
            WHERE workspace=:workspace AND name=:name AND version=:version
            """,
            workflow_dict,
        )
        cur.execute(
            """
            DELETE FROM workflow_dag_node
            WHERE workflow_workspace=:workspace
            AND   workflow_name=:name
            AND   workflow_version=:version
            """,
            workflow_dict,
        )
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


def get_one_workflow(workspace, name, version=None):
    # replace None with '', since None will lead to issues in SQL
    version = version or ''

    with db_connection:
        cur = db_connection.cursor()
        if version == '':
            # Get the head version
            cur.execute(
                """
                SELECT workspace, name, version, timestamp, params, flag, dag
                FROM   workflow 
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
            workflow = cur.fetchone()
            if workflow is None:
                # If there is no head version, get the latest version (by set version to None)
                version = 'latest'
        if version == 'latest':
            # Get the latest version
            cur.execute(
                """
                SELECT workspace, name, version, timestamp, params, flag, dag
                FROM   workflow
                WHERE  workspace=:workspace AND name=:name
                ORDER BY version DESC
                LIMIT 1
                ;
                """,
                {
                    'workspace': workspace,
                    'name': name,
                },
            )
            workflow = cur.fetchone()
        elif version != '':
            cur.execute(
                """
                SELECT workspace, name, version, timestamp, params, flag, dag
                FROM   workflow
                WHERE  workspace=:workspace AND name=:name AND version=:version
                ;
                """,
                {
                    'workspace': workspace,
                    'name': name,
                    'version': version,
                },
            )
            workflow = cur.fetchone()
        workflow_dict = {
            'workspace': workflow[0],
            'name': workflow[1],
            'version': workflow[2] if workflow[2] != '' else None,
            'timestamp': workflow[3],
            'params': json.loads(workflow[4]),
            'flag': json.loads(workflow[5]),
            'dag': {
                'edge': json.loads(workflow[6]),
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


def get_many_workflow(workspace, name, version=None):
    # replace None with '', since None will lead to issues in SQL
    version = version or ''
    with db_connection:
        cur = db_connection.cursor()
        if version == '':
            # Get the head version
            cur.execute(
                """
                SELECT workspace, name, version, timestamp, params, flag, dag
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
                SELECT workspace, name, version, timestamp, params, flag, dag
                FROM   workflow
                WHERE  workspace=:workspace
                AND    name GLOB :name
                ORDER BY version DESC
                LIMIT 1
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
                SELECT workspace, name, version, timestamp, params, flag, dag
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
                'dag': {
                    'edge': json.loads(workflow[6]),
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
