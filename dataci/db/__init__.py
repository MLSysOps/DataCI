#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Mar 09, 2023
"""
import sqlite3 as sl
from pathlib import Path

DB_FILE_PATH = Path(__file__).parents[1].resolve() / 'mydb.db'
db_connection = sl.connect(DB_FILE_PATH)
