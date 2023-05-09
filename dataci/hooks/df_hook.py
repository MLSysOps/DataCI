#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanming.li@alibaba-inc.com
Date: May 05, 2023
"""
from tempfile import NamedTemporaryFile

from dataci.models import Dataset
from dataci.models.workspace import Workspace


class DataFrameHook(object):
    @staticmethod
    def read(dataset_identifier, **context):
        import pandas as pd

        dataset = Dataset.get(dataset_identifier)
        df_path = dataset.dataset_files
        # Set the dataset as the input datasets
        context['input_dataset'] = dataset
        return pd.read_csv(df_path)

    @staticmethod
    def save(name, df, **context):
        workspace, name = name.split('.') if '.' in name else (None, name)
        workspace = Workspace(workspace)
        # Save tmp dataset files to workspace tmp dir
        with NamedTemporaryFile('w', suffix='.csv', delete=False, dir=workspace.tmp_dir) as f:
            df.to_csv(f, index=False)
            dataset = Dataset(
                name,
                dataset_files=f.name,
                yield_workflow=context['workflow'],
                parent_dataset=context['input_dataset'],
            ).save()

        # Set the dataset as the output datasets
        context['output_dataset'] = dataset

        return dataset.identifier
