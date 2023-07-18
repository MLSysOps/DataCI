#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Jul 18, 2023
"""
import datetime
from textwrap import dedent

from pyhive import hive


class Simulator:
    def __init__(self, exp_name, start_time, timescale=1.0, lifecycle=365):
        self.exp_name = exp_name
        self._time = start_time
        self._timescale = timescale
        self._lifecycle = lifecycle

    @classmethod
    def create_simulation(cls, exp_name, source_dataset, start_time, scale=1.0, lifecycle=365):
        # Create simulation at a separated project
        # 1. Copy source dataset @ ds <= start_time
        # 2. Copy downstream dataset and artifact only depends on source dataset @ ds <= start_time
        conn = hive.connect(host='localhost', port=10000, auth='NONE')
        with conn.cursor() as cur:
            cur.execute(f'CREATE DATABASE IF NOT EXISTS {exp_name}')

        with conn.cursor() as cur:
            # Copy source dataset @ ds <= start_time
            latest_ds = start_time.strftime('%Y%m%d')
            oldest_ds = (start_time - datetime.timedelta(lifecycle)).strftime('%Y%m%d')
            print(f'Copying {source_dataset} (ds={oldest_ds} - {latest_ds})')

            exec_sql = dedent("""
            CREATE TABLE IF NOT EXISTS {exp_name}.{source_dataset} LIKE {source_dataset};

            SET hive.exec.max.dynamic.partitions=100000;
            SET hive.exec.max.dynamic.partitions.pernode=100000;
            
            INSERT OVERWRITE TABLE {exp_name}.{source_dataset} PARTITION (ds)
            SELECT * FROM {source_dataset}
            WHERE ds BETWEEN '{oldest_ds}' AND '{latest_ds}'
            """.format(exp_name=exp_name, source_dataset=source_dataset, oldest_ds=oldest_ds, latest_ds=latest_ds))

            for query in exec_sql.split(';'):
                query = query.strip()
                print('Executing SQL:\n', query)
                cur.execute(query)

        return cls(exp_name, start_time, scale)

    @property
    def time(self):
        return self._time

    def set_time(self):
        pass


if __name__ == '__main__':
    Simulator.create_simulation('exp1', 'yelp_review', datetime.date(2020, 7, 13), lifecycle=365)
