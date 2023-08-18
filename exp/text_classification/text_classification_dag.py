#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Jul 30, 2023
"""
import os
from datetime import datetime

from dataci.plugins.decorators import Dataset, stage, dag
from dataci.models import Event
from exp.text_classification.data_augmentation import text_augmentation
from exp.text_classification.data_selection import select_data
from exp.text_classification.predict import main as predict_text_classification
from exp.text_classification.train import main as train_text_classification


@stage
def config_train_args(train_dataset_path, val_dataset_path):
    return [f'--train_dataset={train_dataset_path}', f'--test_dataset={val_dataset_path}']


@stage
def config_predict_args(train_output_path, test_dataset_path):
    return [f'--test_dataset={test_dataset_path}', f'--model_name={os.path.join(train_output_path, "model")}']


@dag(
    start_date=datetime(2020, 7, 30),
    trigger=[Event('publish', 'yelp_review_test@202010_test', producer_type='dataset', status='success')],
)
def text_classification():
    raw_dataset_train = Dataset.get('yelp_review_test@202010_test')
    raw_dataset_val = Dataset.get('yelp_review_test@202010_test', file_reader=None)

    text_aug_df = text_augmentation(raw_dataset_train)
    text_aug_dataset = Dataset(name='text_aug', dataset_files=text_aug_df)
    data_selection_df = select_data(text_aug_dataset, 5, 'bert-base-uncased')
    data_select_dataset = Dataset(name='data_selection', dataset_files=data_selection_df, file_reader=None)
    train_output_path = train_text_classification(config_train_args(data_select_dataset, raw_dataset_val))
    predict_text_classification(config_predict_args(train_output_path, raw_dataset_val))


if __name__ == '__main__':
    dag = text_classification()
    dag.publish()
    # dag.test()
