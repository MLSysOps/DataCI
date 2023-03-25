#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Mar 25, 2023
"""


def create_one_benchmark(benchmark_dict: dict):
    """Create one benchmark in DB
    """
    from . import db_connection

    with db_connection:
        cursor = db_connection.cursor()
        cursor.execute(
            """
            INSERT INTO benchmark (
                type,
                ml_task,
                train_dataset_name,
                train_dataset_version,
                test_dataset_name,
                test_dataset_version,
                model_name,
                train_kwargs,
                result_dir,
                timestamp
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            (
                benchmark_dict['type'],
                benchmark_dict['ml_task'],
                benchmark_dict['train_dataset_name'],
                benchmark_dict['train_dataset_version'],
                benchmark_dict['test_dataset_name'],
                benchmark_dict['test_dataset_version'],
                benchmark_dict['model_name'],
                benchmark_dict['train_kwargs'],
                benchmark_dict['result_dir'],
                benchmark_dict['timestamp'],
            ),
        )
        return cursor.lastrowid
