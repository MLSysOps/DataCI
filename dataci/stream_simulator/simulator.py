#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Jul 18, 2023
"""
import time
from datetime import datetime, timedelta
from textwrap import dedent

from pyhive import hive
from pyhive.exc import ProgrammingError


class Simulator:
    def __init__(self, exp_name, source_dataset, start_time, timescale=1.0, lifecycle=365):
        self.exp_name = exp_name
        self.source_dataset = source_dataset
        self.start_time = start_time
        self._create_time = datetime.now()
        self._update_time = datetime.combine(datetime.min.date(), datetime.min.time())
        self.timescale = timescale
        self.lifecycle = lifecycle

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
        conn = hive.connect(host='localhost', port=10000, auth='NONE')
        with conn.cursor() as cur:
            cur.execute(f'CREATE DATABASE IF NOT EXISTS {exp_name}')
        conn.close()

        self = cls(
            exp_name=exp_name, source_dataset=source_dataset, start_time=start_time,
            timescale=scale, lifecycle=lifecycle,
        )
        # 1. Set current time to start_time - lifecycle
        self._create_time = datetime.now() + timedelta(days=lifecycle / scale)

        # 2. Use set time to set current time to start_time
        self.set_time(start_time)

        # 3. Reset create_time to now
        self._create_time = datetime.now()

        return self

    @property
    def time(self) -> datetime:
        return self.start_time + timedelta(
            seconds=(datetime.now() - self._create_time).total_seconds() * self.timescale
        )

    def set_time(self, new_time: datetime):
        """Set simulation time forward. The simulation project's datasets and artifact will be updated accordingly.

        This method is also called periodically when the simulation is running.

        Raises:
            ValueError: If new_time is earlier than current time. Set time backward is meaningless, you should create
                a new simulation instead.
        """
        # 1. Modify source dataset
        #     - Copy source dataset @ ds in [last_update_time, new_time] from data sink
        #     - Discard source dataset @ ds <= new_time - lifecycle)
        # 2. TODO: Modify outputs (downstream datasets, artifacts) from scheduled dags in simulated project
        #     - Re-generate the outputs from scheduled dags in [current_time, new_time)
        #       (cache may be used from data sink)
        #     - Discard outputs (downstream datasets) in [current_time - lifecycle, new_time - lifecycle)

        new_create_time = datetime.now() - timedelta(
            seconds=(new_time - self.start_time).total_seconds() / self.timescale
        )

        conn = hive.connect(
            host='localhost', port=10000, auth='NONE',
            configuration={
                'hive.exec.max.dynamic.partitions': '10000',
                'hive.exec.max.dynamic.partitions.pernode': '10000'
            }
        )
        with conn.cursor() as cur:
            # Copy source dataset @ ds in [last_update_time, new_time) from data sink
            latest_ds = new_time.strftime('%Y%m%d')
            cutoff_ds = (new_time - timedelta(days=self.lifecycle)).strftime('%Y%m%d')
            oldest_ds = max(self._update_time.strftime('%Y%m%d'), cutoff_ds)
            print(
                'Create dataset partitions: '
                f'{self.source_dataset}/ds={oldest_ds} (inclusive) - {latest_ds} (exclusive)'
            )

            exec_sql = dedent("""
            CREATE TABLE IF NOT EXISTS {exp_name}.{source_dataset} LIKE {source_dataset};

            INSERT OVERWRITE TABLE {exp_name}.{source_dataset} PARTITION (ds)
            SELECT * FROM {source_dataset}
            WHERE  ds >= '{oldest_ds}' 
            AND    ds < '{latest_ds}'
            """.format(
                exp_name=self.exp_name,
                source_dataset=self.source_dataset,
                oldest_ds=oldest_ds,
                latest_ds=latest_ds
            ))

            for query in exec_sql.split(';'):
                query = query.strip()
                if not query:
                    continue
                print('Executing SQL:\n' + query)
                cur.execute(query)
                print('Result:')
                try:
                    print(cur.fetchall())
                except ProgrammingError:
                    print('No result')

            # Discard source dataset @ ds in [..., new_time - lifecycle)
            print(
                'Delete dataset partitions: '
                f'{self.source_dataset}/ds<{oldest_ds}'
            )
            exec_sql = dedent("""
            ALTER TABLE {exp_name}.{source_dataset} DROP IF EXISTS PARTITION (ds<'{cutoff_ds}');
            """.format(
                exp_name=self.exp_name,
                source_dataset=self.source_dataset,
                cutoff_ds=cutoff_ds,
            ))

            for query in exec_sql.split(';'):
                query = query.strip()
                if not query:
                    continue
                print('Executing SQL:\n' + query)
                cur.execute(query)
                print('Result:')
                try:
                    print(cur.fetchall())
                except ProgrammingError:
                    print('No result')
        self._update_time = new_time
        self._create_time = new_create_time.replace(microsecond=0)

    def run(self, schedule_freq=1):
        """Run the simulation until the simulated time reaches create time.

        Args:
            schedule_freq (int): Scheduling frequency to call :code:`set_time` in days.
                Default to 1 day.
        """
        # set a thread timer to call set_time periodically
        while self.time < self._create_time:
            now = self.time
            print(f'Simulation time: {now}')
            self.set_time(now)
            print('Create time: ' + str(self._create_time))
            time_elapse = self.time - now
            print(f'Simulated time elapse: {time_elapse}')
            # Calculate how much time in real world should be passed
            # 1 day in simulation time = 1 / timescale days in real world
            sleep_time = max((timedelta(days=schedule_freq) - time_elapse).total_seconds() / self.timescale, 0)
            print(f'Sleep for {timedelta(seconds=sleep_time)}')
            time.sleep(sleep_time)


if __name__ == '__main__':
    # sim = Simulator.create_simulation('exp1', 'yelp_review', datetime(2020, 7, 13), lifecycle=5)
    # sim.set_time(datetime(2020, 7, 14))
    sim = Simulator('exp1', 'yelp_review', datetime(2020, 7, 14), timescale=2400, lifecycle=5)
    sim.run()
    # time.sleep(10)
    # print(sim.time)
