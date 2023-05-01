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
                    'stage_version': node['version'],
                    'dag_node_id': node_id,
                }
                for node_id, node in workflow_dag_node_dict.items()
            ],
        )
        return workflow_id


def exist_workflow(workspace, name, version):
    with db_connection:
        cur = db_connection.cursor()
        cur.execute(
            """
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
                    'stage_version': node['version'],
                    'dag_node_id': node_id,
                }
                for node_id, node in workflow_dag_node_dict.items()
            ],
        )
