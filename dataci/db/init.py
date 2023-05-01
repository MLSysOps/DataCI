#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Mar 09, 2023
"""
import logging

from dataci.db import db_connection

logger = logging.getLogger(__name__)

# Drop all tables
with db_connection:
    db_connection.executescript("""
    DROP TABLE IF EXISTS benchmark;
    DROP TABLE IF EXISTS run;
    DROP TABLE IF EXISTS dataset_tag;
    DROP TABLE IF EXISTS dataset;
    DROP TABLE IF EXISTS workflow_dag_node;
    DROP TABLE IF EXISTS stage;
    DROP TABLE IF EXISTS workflow;
    """)
logger.info('Drop all tables.')

# Create dataset table
with db_connection:
    db_connection.executescript("""
    CREATE TABLE workflow
    (
        workspace TEXT,
        name      TEXT,
        version   TEXT,
        timestamp INTEGER,
        params    TEXT,
        flag      TEXT,
        dag       TEXT,
        PRIMARY KEY (workspace, name, version)
    );
    
    CREATE TABLE stage
    (
        workspace   TEXT,
        name        TEXT,
        version     TEXT,
        script_path TEXT,
        timestamp   INTEGER,
        symbolize   TEXT,
        PRIMARY KEY (workspace, name, version)
    );
    
    CREATE TABLE workflow_dag_node
    (
        workflow_workspace TEXT,
        workflow_name      TEXT,
        workflow_version   TEXT,
        stage_workspace    TEXT,
        stage_name         TEXT,
        stage_version      TEXT,
        dag_node_id        INTEGER,
        PRIMARY KEY (workflow_workspace, workflow_name, workflow_version, stage_workspace, stage_name, stage_version),
        FOREIGN KEY (workflow_workspace, workflow_name, workflow_version)
            REFERENCES workflow (workspace, name, version),
        FOREIGN KEY (stage_workspace, stage_name, stage_version)
            REFERENCES stage (workspace, name, version)
    );
    
    CREATE TABLE dataset
    (
        workspace                TEXT,
        name                     TEXT,
        version                  TEXT,
        yield_workflow_workspace TEXT,
        yield_workflow_name      TEXT,
        yield_workflow_version   TEXT,
        log_message              TEXT,
        timestamp                INTEGER,
        id_column                TEXT,
        size                     INTEGER,
        filename                 TEXT,
        parent_dataset_workspace TEXT,
        parent_dataset_name      TEXT,
        parent_dataset_version   TEXT,
        FOREIGN KEY (yield_workflow_workspace, yield_workflow_name, yield_workflow_version)
            REFERENCES workflow (workspace, name, version),
        FOREIGN KEY (parent_dataset_workspace, parent_dataset_name, parent_dataset_version)
            REFERENCES dataset (workspace, name, version),
        PRIMARY KEY (workspace, name, version)
    );
    
    CREATE TABLE dataset_tag
    (
        dataset_name    TEXT,
        dataset_version TEXT,
        tag_name        TEXT,
        tag_version     TEXT,
        FOREIGN KEY (dataset_name, dataset_version) REFERENCES dataset (name, version),
        PRIMARY KEY (tag_name, tag_version)
    );

    CREATE TABLE run
    (
        run_num INTEGER,
        workflow_name TEXT,
        workflow_version TEXT,
        FOREIGN KEY (workflow_name, workflow_version) REFERENCES workflow (name, version),
        PRIMARY KEY (workflow_name, workflow_version, run_num)
    );
    
    CREATE TABLE benchmark
    (
        type                  TEXT,
        ml_task               TEXT,
        train_dataset_name    TEXT,
        train_dataset_version TEXT,
        test_dataset_name     TEXT,
        test_dataset_version  TEXT,
        model_name            TEXT,
        train_kwargs          TEXT,
        result_dir            TEXT,
        timestamp             INTEGER,
        FOREIGN KEY (train_dataset_name, train_dataset_version) REFERENCES dataset (name, version),
        FOREIGN KEY (test_dataset_name, test_dataset_version) REFERENCES dataset (name, version)
    );
    """)
logger.info('Create all tables.')
