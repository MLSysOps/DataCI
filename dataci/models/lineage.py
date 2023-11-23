#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Nov 22, 2023
"""
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import List, Optional, Union

    from dataci.models import Dataset, Workflow, Stage, Run

class Lineage(object):

    def __init__(
            self,
            run: 'Union[Run, dict]',
            parent_run: 'Optional[Union[Run, dict]]' = None,
            inputs: 'List[Union[Dataset, dict]]' = None,
            outputs: 'List[Union[Dataset, dict]]' = None,
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
            self._run = Run.get(self._run['run_id'])
        return self._run

    @property
    def parent_run(self) -> 'Optional[Run]':
        """Lazy load parent run from database."""
        from dataci.models import Run

        if self._parent_run is None:
            return None

        if not isinstance(self._parent_run, Run):
            self._parent_run = Run.get(self._parent_run['run_id'])
        return self._parent_run

    @property
    def inputs(self) -> 'List[Dataset]':
        """Lazy load inputs from database."""
        inputs = list()
        for input_ in self._inputs:
            if not isinstance(input_, Dataset):
                dataset_id = input_['workspace'] + '.' + input_['name'] + '@' + input_['version']
                inputs.append(Dataset.get(dataset_id))
            else:
                inputs.append(input_)
        self._inputs = inputs
        return self._inputs

    @property
    def outputs(self) -> 'List[Dataset]':
        """Lazy load outputs from database."""
        outputs = list()
        for output in self._outputs:
            if not isinstance(output, Dataset):
                dataset_id = output['workspace'] + '.' + output['name'] + '@' + output['version']
                outputs.append(Dataset.get(dataset_id))
            else:
                outputs.append(output)
        self._outputs = outputs
        return self._outputs

