#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Mar 09, 2023
"""
from dataci.db import db_connection

# Drop all tables
with db_connection:
    db_connection.executescript("""
    DROP TABLE IF EXISTS run;
    DROP TABLE IF EXISTS dataset_tag;
    DROP TABLE IF EXISTS dataset;
    DROP TABLE IF EXISTS pipeline;
    """)

# Create dataset table
with db_connection:
    db_connection.executescript("""
    CREATE TABLE pipeline
    (
        name      TEXT,
        version   TEXT,
        timestamp INTEGER,
        PRIMARY KEY (name, version)
    );
    
    CREATE TABLE dataset
    (
        name                   TEXT,
        version                TEXT,
        yield_pipeline_name    TEXT,
        yield_pipeline_version TEXT,
        log_message            TEXT,
        timestamp              INTEGER,
        id_column              TEXT,
        size                   INTEGER,
        file_config            TEXT,
        filename               TEXT,
        parent_dataset_name    TEXT,
        parent_dataset_version TEXT,
        FOREIGN KEY (yield_pipeline_name, yield_pipeline_version) REFERENCES pipeline (name, version),
        FOREIGN KEY (parent_dataset_name, parent_dataset_version) REFERENCES dataset (name, version),
        PRIMARY KEY (name, version)
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
        pipeline_name TEXT,
        pipeline_version TEXT,
        FOREIGN KEY (pipeline_name, pipeline_version) REFERENCES pipeline (name, version),
        PRIMARY KEY (pipeline_name, pipeline_version, run_num)
    );
    """)
