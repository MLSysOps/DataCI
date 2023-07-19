#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Jul 18, 2023
"""
import time
from datetime import datetime
from textwrap import dedent

from pyhive import hive


class Simulator:
    def __init__(self, exp_name, start_time, timescale=1.0, lifecycle=365):
        self.exp_name = exp_name
        self._start_time = start_time
        self._create_time = datetime.now()
        self._timescale = timescale
        self._lifecycle = lifecycle

    @classmethod
    def create_simulation(
            cls,
            exp_name,
            source_dataset,
            start_time: datetime,
            scale: float = 1.0,
            lifecycle: int = 365
    ):
        # Create simulation at a separated project
        # 1. Copy source dataset @ ds in [start_time - lifecycle, start_time]
        # 2. Copy downstream dataset and artifact (code, models) that
        #   only depends on source dataset @ ds [start_time - lifecycle, start_time]
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

            print('Executing SQL:\n', exec_sql)
            for query in exec_sql.split(';'):
                cur.execute(query.strip())
                print('Result:\n', cur.fetchall())

        return cls(exp_name, start_time, scale)

    @property
    def time(self) -> datetime:
        return self._start_time + (datetime.now() - self._create_time) * self._timescale

    def set_time(self, new_time: datetime):
        """Set simulation time forward. The simulation project's datasets and artifact will be updated accordingly.

        This method is also called periodically when the simulation is running.

        Raises:
            ValueError: If new_time is earlier than current time. Set time backward is meaningless, you should create
                a new simulation instead.
        """
        # 1. Modify source dataset
        #     - Copy source dataset @ ds in [current_time, new_time] from data sink
        #     - Discard source dataset @ ds in [current_time - lifecycle, new_time - lifecycle]
        # 2. Modify outputs (downstream datasets, artifacts) from scheduled dags in simulated project
        #     - Re-generate the outputs from scheduled dags in [current_time, new_time]
        #       (cache may be used from data sink)
        #     - Discard outputs (downstream datasets) in [current_time - lifecycle, new_time - lifecycle]
        pass

    def run(self):
        """Run the simulation until the simulated time reaches create time."""
        pass


if __name__ == '__main__':
    # Simulator.create_simulation('exp1', 'yelp_review', datetime.date(2020, 7, 13), lifecycle=365)
    sim = Simulator('exp1', datetime(2020, 7, 13), timescale=2)
    print(sim.time)
    time.sleep(10)
    print(sim.time)
