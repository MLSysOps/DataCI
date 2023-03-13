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
