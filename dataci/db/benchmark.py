#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Mar 25, 2023
"""
from . import db_connection


def create_one_benchmark(benchmark_dict: dict):
    """Create one benchmark in DB
    """

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


def get_many_benchmarks(train_dataset_name: str):
    """List all benchmarks
    """
    with db_connection:
        cursor = db_connection.cursor()
        cursor.execute(
            """
            SELECT type
                 , ml_task
                 , train_dataset_name
                 , train_dataset_version
                 , test_dataset_name
                 , test_dataset_version
                 , model_name
                 , train_kwargs
                 , result_dir
                 , timestamp 
            FROM benchmark
            WHERE train_dataset_name GLOB ?
            ;
            """,
            (train_dataset_name,),
        )
        benchmark_lists = cursor.fetchall()

    benchmark_dicts = list()
    for benchmark in benchmark_lists:
        benchmark_dicts.append({
            'type': benchmark[0],
            'ml_task': benchmark[1],
            'train_dataset': {
                'name': benchmark[2],
                'version': benchmark[3],
            },
            'test_dataset': {
                'name': benchmark[4],
                'version': benchmark[5],
            },
            'model_name': benchmark[6],
            'train_kwargs': benchmark[7],
            'result_dir': benchmark[8],
            'timestamp': benchmark[9],
        })

    return benchmark_dicts
