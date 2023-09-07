#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com.com
Date: Aug 28, 2023
"""
from dataci.models import Dataset

print('Download and publish latest dataset version 2020-10...')
DATA_URL_BASE = 'https://zenodo.org/record/8288433/files'

yelp_review_train = Dataset(
    'yelp_review_train',
    dataset_files=f'{DATA_URL_BASE}/yelp_review_train_2020-10.csv').publish('2020-10')
yelp_review_val = Dataset(
    'yelp_review_val',
    dataset_files=f'{DATA_URL_BASE}/yelp_review_val_2020-10.csv').publish('2020-10')
print(yelp_review_train, yelp_review_val)
