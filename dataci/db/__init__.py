#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Mar 09, 2023
"""
import sqlite3 as sl

from dataci.config import DB_FILE

# FIXME: There are known issue for sqlite in a multi-threaded environment
#  error will be caused for streamlit caching
#  https://stackoverflow.com/questions/524797/python-sqlite-and-threading
#  Set `check_same_thread` is a temp solution, need to fix this issue

db_connection = sl.connect(DB_FILE, check_same_thread=False)
