#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Jun 11, 2023
"""
from airflow.models import DAG as _DAG
from airflow.operators.python import PythonOperator as _PythonOperator

from dataci.models import Workflow, Stage


class DAG(Workflow, _DAG):
    """A wrapper class for :code:`airflow.models.DAG`. Substituting the
    :code:`airflow.models.DAG` class with this class to provide version control
    over the DAGs.
    """
    BACKEND = 'airflow'

    def __init__(self, *args, **kwargs):
        name = kwargs.get('dag_id', None) or args[0]
        super().__init__(name, *args, **kwargs)

    @property
    def stages(self):
        return self.tasks


class PythonOperator(Stage, _PythonOperator):
    name_arg = 'task_id'

    def __int__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
