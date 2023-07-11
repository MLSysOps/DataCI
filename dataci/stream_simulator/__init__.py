#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Jul 05, 2023
"""
from pyhive import hive


with hive.connect(host='localhost', port=10000, auth='NONE') as conn:
    with conn.cursor() as cur:
        # Show databases
        cur.execute('show databases')
        print(cur.fetchall())

        # # Execute query
        # cur.execute("select * from t1")
        #
        # # Return column info from query
        # print cur.getSchema()
        #
        # # Fetch table results
        # for i in cur.fetch():
        #     print i
