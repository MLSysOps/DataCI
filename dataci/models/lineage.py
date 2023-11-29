#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Nov 22, 2023
"""
import sqlite3
import warnings
from typing import TYPE_CHECKING

from dataci.config import DB_FILE
from dataci.db.lineage import (
    exist_run_lineage, exist_many_run_dataset_lineage, create_one_run_lineage, create_many_dataset_lineage
)
from dataci.models.dataset import Dataset

if TYPE_CHECKING:
    from typing import List, Optional, Union

    from dataci.models import Workflow, Stage, Run

class Lineage(object):

    def __init__(
            self,
            run: 'Union[Run, dict]',
            parent_run: 'Optional[Union[Run, dict]]' = None,
            inputs: 'List[Union[Dataset, dict, str]]' = None,
            outputs: 'List[Union[Dataset, dict, str]]' = None,
    ):
        self._run = run
        self._parent_run = parent_run
        self._inputs: 'List[Dataset]' = inputs or list()
        self._outputs: 'List[Dataset]' = outputs or list()

    def dict(self):
        return {
            'parent_run': self.parent_run.dict(id_only=True) if self.parent_run else None,
            'run': self.run.dict() if self.run else None,
            'inputs': [input_.dict(id_only=True) for input_ in self.inputs],
            'outputs': [output.dict(id_only=True) for output in self.outputs],
        }

    @classmethod
    def from_dict(cls, config):
        pass

    @property
    def job(self) -> 'Union[Workflow, Stage]':
        return self.run.job

    @property
    def run(self) -> 'Run':
        """Lazy load run from database."""
        from dataci.models import Run

        if not isinstance(self._run, Run):
            self._run = Run.get(self._run['name'])
        return self._run

    @property
    def parent_run(self) -> 'Optional[Run]':
        """Lazy load parent run from database."""
        from dataci.models import Run

        if self._parent_run is None:
            return None

        if not isinstance(self._parent_run, Run):
            self._parent_run = Run.get(self._parent_run['name'])
        return self._parent_run

    @property
    def inputs(self) -> 'List[Dataset]':
        """Lazy load inputs from database."""
        inputs = list()
        for input_ in self._inputs:
            if isinstance(input_, Dataset):
                inputs.append(input_)
            elif isinstance(input_, dict):
                inputs.append(Dataset.get(**input_))
            else:
                warnings.warn(f'Unable to parse input {input_}')
        self._inputs = inputs
        return self._inputs

    @property
    def outputs(self) -> 'List[Dataset]':
        """Lazy load outputs from database."""
        outputs = list()
        for output in self._outputs:
            if isinstance(output, Dataset):
                outputs.append(output)
            elif isinstance(output, dict):
                outputs.append(Dataset.get(**output))
            else:
                warnings.warn(f'Unable to parse output {output}')
        self._outputs = outputs
        return self._outputs

    def save(self, exist_ok=True):
        config = self.dict()
        for input_ in config['inputs']:
            input_['direction'] = 'input'
        for output in config['outputs']:
            output['direction'] = 'output'

        run_lineage_exist = (config['parent_run'] is None) or exist_run_lineage(
            config['run']['name'],
            config['run']['version'],
            config['parent_run'].get('name', None),
            config['parent_run'].get('version', None)
        )

        dataset_lineage_exist_list = exist_many_run_dataset_lineage(
            config['run']['name'],
            config['run']['version'],
            config['inputs'] + config['outputs']
        )

        # Check if run lineage exists
        is_create_run_lineage = False
        if run_lineage_exist:
            if not exist_ok:
                raise ValueError(f'Run lineage {self.parent_run} -> {self.run} exists.')
        else:
            # Set create run lineage to True
            is_create_run_lineage = True

        # Check if dataset lineage exists
        if any(dataset_lineage_exist_list):
            if not exist_ok:
                raise ValueError(f'Dataset lineage exists.')
            else:
                # Remove the existed dataset lineage
                config['inputs'] = [
                    dataset_config for dataset_config, exist in zip(
                        config['inputs'], dataset_lineage_exist_list[:len(config['inputs'])]
                    ) if not exist
                ]
                config['outputs'] = [
                    dataset_config for dataset_config, exist in zip(
                        config['outputs'], dataset_lineage_exist_list[len(config['inputs']):]
                    ) if not exist
                ]

        with sqlite3.connect(DB_FILE) as conn:
            cur = conn.cursor()
            # Create run lineage
            if is_create_run_lineage:
                create_one_run_lineage(config, cur)
            # Create dataset lineage
            create_many_dataset_lineage(config, cur)

        return self

    def get(self, run_name, run_version):
        pass
