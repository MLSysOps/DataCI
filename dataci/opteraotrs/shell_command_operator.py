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

    def run(self, *inputs, **kwargs):
        import subprocess

        # parse variables ${{ }} in command
        # 1. Locate all variables
        # 2. Replace all variables with values
        # 3. Run command

        command = self.params['command']
        print('Executing command: ', command)
        subprocess.run(command, shell=True)
