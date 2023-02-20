#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Feb 20, 2023
"""
from abc import ABC, abstractclassmethod

import pandas as pd


class Stage(ABC):
    def __init__(self, name: str, inputs: str, outputs: str, dependency='auto') -> None:
        self.name = name
        self.inputs = inputs
        self.outputs = outputs
        self.dependency = dependency
    
    def resolve(self):
        """Resolve shortcut init arguements"""
        # Resolve input: find the dataset path
        
        # Resolve outputs: 
        
        # Resolve dependencies
        
        # Resolve run function
    
    @staticmethod
    @abstractclassmethod
    def run(data):
        raise NotImplementedError('Method `run` not implemented.')

    def __call__(self):
        # Read input
        df = pd.read_csv(self.inputs)
        
        # Execute user override :code:`run` function
        outputs = df.apply(self.run(df), axis=1)
        
        # Dump output
        outputs.to_csv(self.outputs, index=False)
