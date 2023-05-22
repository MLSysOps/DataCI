#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: May, 23, 2023

1. Download dataset from Google Drive
2. Unzip dataset to current directory / dataset base directory
"""
from datetime import datetime

import pandas as pd
from alaas.server import Server
from dateutil.relativedelta import relativedelta

# DATASET_BASE_PATH = '/data/yelp/reviews_{}.csv'
DATASET_BASE_PATH = 'reviews_{}.csv'


def read_dataset(start_date: str, end_date: str):
    start_date = datetime.strptime(start_date, '%Y%m')
    end_date = datetime.strptime(end_date, '%Y%m')
    month_cnt = (end_date.year - start_date.year) * 12 + (end_date.month - start_date.month) + 1

    df_list = list()
    for i in range(month_cnt):
        date = start_date + relativedelta(months=i)
        date_str = date.strftime('%Y%m')
        df_list.append(pd.read_csv(DATASET_BASE_PATH.format(date_str)))
    return pd.concat(df_list, ignore_index=True)


if __name__ == '__main__':
    Server.start(
        model_hub="huggingface/pytorch-transformers",
        model_name="bert-base-uncased",
        tokenizer="bert-base-uncased",
        transformers_task="text-classification",
        strategy="RandomSampling"
    )
