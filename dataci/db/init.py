#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Mar 09, 2023
"""
import logging
import sqlite3

from dataci.config import DB_FILE

logger = logging.getLogger(__name__)

db_connection = sqlite3.connect(DB_FILE)

# Drop all tables
with db_connection:
    db_connection.executescript("""
    DROP TABLE IF EXISTS lineage;
    DROP TABLE IF EXISTS run;
    DROP TABLE IF EXISTS dataset_tag;
    DROP TABLE IF EXISTS dataset;
    DROP TABLE IF EXISTS workflow_dag_node;
    DROP TABLE IF EXISTS stage_tag;
    DROP TABLE IF EXISTS stage;
    DROP TABLE IF EXISTS workflow_tag;
    DROP TABLE IF EXISTS workflow;
    DROP TABLE IF EXISTS job;
    """)
logger.info('Drop all tables.')

# Create dataset table
with db_connection:
    db_connection.executescript("""
    CREATE TABLE job
    (
        workspace TEXT,
        name      TEXT,
        version   TEXT,
        type      TEXT,
        PRIMARY KEY (workspace, name, version, type),
        UNIQUE (workspace, name, version, type)
    );
    
    CREATE TABLE workflow
    (
        workspace   TEXT,
        name        TEXT,
        version     TEXT,
        timestamp   INTEGER,
        params      TEXT,
        flag        TEXT,
        schedule    TEXT,
        dag         TEXT,
        script_dir       TEXT,
        script_entry     TEXT,
        script_filelist  TEXT,
        script_hash      TEXT,
        PRIMARY KEY (workspace, name, version),
        UNIQUE (workspace, name, version),
        FOREIGN KEY (workspace, name, version)
            REFERENCES job (workspace, name, version)
    );
    
    CREATE TABLE workflow_tag
    (
        workspace TEXT,
        name      TEXT,
        version   TEXT,
        tag       INTEGER,
        PRIMARY KEY (workspace, name, tag),
        UNIQUE (workspace, name, tag),
        UNIQUE (workspace, name, version),
        FOREIGN KEY (workspace, name, version)
            REFERENCES workflow (workspace, name, version)
    );
    
    CREATE TABLE stage
    (
        workspace        TEXT,
        name             TEXT,
        version          TEXT,
        params           TEXT,
        timestamp        INTEGER,
        script_dir       TEXT,
        script_entry     TEXT,
        script_filelist  TEXT,
        script_hash      TEXT,
        PRIMARY KEY (workspace, name, version),
        UNIQUE (workspace, name, version),
        FOREIGN KEY (workspace, name, version)
            REFERENCES job (workspace, name, version)
    );
    
    CREATE TABLE stage_tag
    (
        workspace TEXT,
        name      TEXT,
        version   TEXT,
        tag       INTEGER,
        PRIMARY KEY (workspace, name, tag),
        UNIQUE (workspace, name, tag),
        UNIQUE (workspace, name, version),
        FOREIGN KEY (workspace, name, version)
            REFERENCES stage (workspace, name, version)
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
        dag_node_path      TEXT,
        PRIMARY KEY (workflow_workspace, workflow_name, workflow_version, stage_workspace, stage_name, stage_version),
        UNIQUE (workflow_workspace, workflow_name, workflow_version, dag_node_id),
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
        log_message              TEXT,
        timestamp                INTEGER,
        id_column                TEXT,
        size                     INTEGER,
        location                 TEXT,
        UNIQUE (workspace, name, version)
    );
    
    CREATE TABLE dataset_tag
    (
        workspace TEXT,
        name      TEXT,
        version   TEXT,
        tag       TEXT,
        timestamp INTEGER,
        PRIMARY KEY (workspace, name, tag),
        UNIQUE (workspace, name, tag),
        UNIQUE (workspace, name, version),
        FOREIGN KEY (workspace, name, version)
            REFERENCES dataset (workspace, name, version)
    );
    
    CREATE TABLE run
    (
        workspace     TEXT,
        name          TEXT,
        version       INTEGER,
        status        TEXT,
        job_workspace TEXT,
        job_name      TEXT,
        job_version   TEXT,
        job_type      TEXT,
        create_time   INTEGER,
        update_time   INTEGER,
        PRIMARY KEY (workspace, name, version),
        UNIQUE (name, version),
        FOREIGN KEY (workspace, name, version)
            REFERENCES job (workspace, name, version),
        FOREIGN KEY (job_workspace, job_name, job_version, job_type)
            REFERENCES job (workspace, name, version, type)
    );
    
    CREATE TABLE lineage
    (
        upstream_workspace TEXT,
        upstream_name      TEXT,
        upstream_version   TEXT,
        upstream_type      TEXT,
        downstream_workspace TEXT,
        downstream_name      TEXT,
        downstream_version   TEXT,
        downstream_type      TEXT,
        PRIMARY KEY (upstream_workspace, upstream_name, upstream_version, upstream_type, downstream_workspace, downstream_name, downstream_version, downstream_type),
        FOREIGN KEY (upstream_workspace, upstream_name, upstream_version, upstream_type)
            REFERENCES job (workspace, name, version, type),
        FOREIGN KEY (downstream_workspace, downstream_name, downstream_version, downstream_type)
            REFERENCES job (workspace, name, version, type)
    );
    """)
logger.info('Create all tables.')
