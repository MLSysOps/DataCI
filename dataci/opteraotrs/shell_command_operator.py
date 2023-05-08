#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: May 08, 2023
"""
import re

from dataci.models import Stage


class ShellCommandOperator(Stage):

    def __init__(self, name: str, symbolize: str = None, params: dict = None, command: str = '', **kwargs, ):
        super().__init__(
            name=name,
            symbolize=symbolize,
            params=params,
            **kwargs,
        )
        self.command = command

    def run(self, *inputs, **kwargs):
        import subprocess

        # parse variables ${{ }} in command
        # 1. Locate all variables
        # 2. Replace all variables with values
        # 3. Run command

        groups = re.findall(r'\${{.*}}', self.command, re.M | re.I)
        for group in groups:
            var = group.strip('${').strip('}')
            self.command = self.command.replace(group, eval(var))

        subprocess.run(self.command, shell=True)
