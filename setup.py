#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanming.li@alibaba-inc.com
Date: May 11, 2023
"""
import sys

from setuptools import setup

PYTHON_VERSION = str(sys.version_info[0]) + '.' + str(sys.version_info[1])
ORCHESTRATION_BACKEND = 'airflow'

with open('requirements.txt') as f:
    requirements = f.read().splitlines()
constraints = None

if ORCHESTRATION_BACKEND == 'airflow':
    AIRFLOW_VERSION = '2.6.1'
    requirements.append(f'apache-airflow[celery]=={AIRFLOW_VERSION}')
    constraints = f'https://raw.githubusercontent.com/apache/airflow/constraints-{AIRFLOW_VERSION}/' \
                  f'constraints-{PYTHON_VERSION}.txt'

setup(
    name="dataci",
    version='0.0.1',
    author="Yuanming Li",
    author_email="yuanmingleee@gmail.com",
    py_modules=['dataci'],
    install_requires=requirements,
    constraints=constraints,
    entry_points="""
        [console_scripts]
        dataci=dataci.command:cli
    """,
)
