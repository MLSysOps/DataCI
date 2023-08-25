#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Aug 11, 2023
"""
from dataci.models import Dataset

dataset = Dataset(name='yelp_review_test', dataset_files='data/reviews_202010_test.csv')
dataset.save()
dataset.publish('202010_test')
# print(Dataset.get('yelp_review_train@2020Q4').read())
