#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Jul 30, 2023
"""
from dataci.decorators import Dataset, stage, dag

from exp.text_classification.data_augmentation import text_augmentation
from exp.text_classification.data_selection import select_data
from exp.text_classification.train import main as train_text_classification


@stage
def config_train_text_classification_args(dataset_path):
    return [f'--train_dataset={dataset_path}']


@dag()
def text_classification():
    raw_dataset = Dataset.get('yelp_review@v1')
    text_aug_df = text_augmentation(raw_dataset)
    text_aug_dataset = Dataset(name='text_aug', data_files=text_aug_df)
    data_selection_df = select_data(text_aug_dataset)
    data_select_dataset = Dataset(name='data_selection', data_files=data_selection_df, file_reader=None)
    train_args = config_train_text_classification_args(data_select_dataset)
    train_text_classification(train_args)



if __name__ == '__main__':
    dag = text_classification()
    dag.test()
