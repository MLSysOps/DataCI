#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Mar 13, 2023
"""
from . import db_connection


def create_one_pipeline(pipeline_dict):
    with db_connection:
        # Publish pipeline
        db_connection.execute(
            """
            INSERT INTO pipeline (name, version) VALUES (?,?)
            """,
            (pipeline_dict['name'], pipeline_dict['version']),
        )


def create_one_pipeline_run(run_dict, outputs_dict):
    with db_connection:
        pipeline_dict = run_dict['pipeline']
        # Publish run
        db_connection.execute(
            """
            INSERT INTO run(run_num, pipeline_name, pipeline_version) VALUES(?,?,?)
            """
            , (run_dict['run_num'], pipeline_dict['name'], pipeline_dict['version'])
        )

        # Publish pipeline output dataset
        for output_dataset in outputs_dict:
            db_connection.execute(
                """
                INSERT INTO dataset (name, version, yield_pipeline_name, yield_pipeline_version, log_message, 
                timestamp, filename, file_config, parent_dataset_name, parent_dataset_version)
                VALUES (?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    output_dataset['name'], output_dataset['version'], pipeline_dict['name'], pipeline_dict['version'],
                    output_dataset['log_message'], output_dataset['timestamp'], output_dataset['filename'],
                    output_dataset['file_config'], output_dataset['parent_dataset_name'],
                    output_dataset['parent_dataset_version'],
                )
            )
