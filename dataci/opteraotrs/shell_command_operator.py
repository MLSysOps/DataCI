#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: May 08, 2023
"""
from dataci.models import Stage


class ShellCommandOperator(Stage):

    def __init__(self, name: str, symbolize: str = None, params: dict = None, command: str = '', **kwargs, ):
        super().__init__(
            name=name,
            symbolize=symbolize,
            params=params,
            **kwargs,
        )
        if command:
            self.params['command'] = command

    def run(self, *inputs, **context):
        import subprocess
        import re

        # parse variables ${{ }} in command
        # 1. Locate all variables
        # 2. Replace all variables with values
        # 3. Run command

        command = self.params['command']
        # Locate runtime variables
        matched = re.findall(r'\$\{.*?}', command)
        for var in matched:
            var_name = var[2:-1].strip()
            # get stage name
            stage_name = var_name.split('.')[0]
            # get variable value
            value = context.get('outputs', dict()).get(stage_name, dict())
            command = command.replace(var, value)
        print('Executing command: ', command)
        ret = subprocess.run(command, shell=True)
        if ret.returncode != 0:
            raise Exception(f'Failed to execute command: {command}')
