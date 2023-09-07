#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: May 11, 2023
"""
from setuptools import setup, find_namespace_packages

with open('requirements.txt') as f:
    requirements = f.read().splitlines()

setup(
    name="dataci",
    version='0.0.1',
    author="Yuanming Li",
    author_email="yuanmingleee@gmail.com",
    license='MIT',
    py_modules=['dataci'],
    packages=find_namespace_packages(exclude=['tests', 'exp', 'example']),
    install_requires=requirements,
    entry_points="""
        [console_scripts]
        dataci=dataci.command:cli
    """,
)
