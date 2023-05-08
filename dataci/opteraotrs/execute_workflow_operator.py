#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: May 08, 2023
"""
from dataci.models import Stage


class ExecuteWorkflowOperator(Stage):
    def __init__(self, name: str, symbolize: str = None, params: dict = None, workflow_identifier=..., **kwargs, ):
        super().__init__(
            name=name,
            symbolize=symbolize,
            params=params,
            **kwargs,
        )
        self.workflow_identifier = workflow_identifier

    def run(self, **context):
        from dataci.models import Workflow

        workflow_obj = Workflow.get(self.workflow_identifier)
        # set workflow params
        workflow_obj.params = context['params']
        return workflow_obj()
