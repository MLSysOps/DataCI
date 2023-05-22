#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: May 22, 2023
"""
import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import pandas as pd

print_freq = 100000
BASE_DIR = Path(r'C:\Users\test\Downloads\yelp')


def partition_dataset_stat():
    with open(BASE_DIR / 'yelp_academic_dataset_review.json', 'r', encoding='utf-8') as f:
        ts = []
        for i, line in enumerate(f):
            review_json = json.loads(line)
            timestamp = datetime.strptime(review_json['date'], '%Y-%m-%d %H:%M:%S')
            ts.append(timestamp.date().strftime('%Y%m%d'))
            if i > 0 and i % print_freq == 0:
                print(f'Finish reading {i} lines')
    print('Finish reading')
    df = pd.DataFrame(ts, columns=['timestamp'])
    # Partition by review day
    df = df.groupby(['timestamp']).size().reset_index(name='count')
    df.to_csv('timestamp.csv', index=False)

    # Partition by review month
    df = pd.read_csv('timestamp.csv')
    df['year_month'] = df['timestamp'].apply(lambda x: str(x)[:6])
    df = df.groupby(['year_month']).sum().reset_index()
    df.to_csv('timestamp_month.csv', index=False)


def partition_dataset_by_month(start_month='202010'):
    reviews_month_dict = defaultdict(list)
    with open(BASE_DIR / 'yelp_academic_dataset_review.json', 'r', encoding='utf-8') as f:
        for i, line in enumerate(f):
            review_json = json.loads(line)
            month = datetime.strptime(review_json['date'], '%Y-%m-%d %H:%M:%S').date().strftime('%Y%m')
            if month >= start_month:
                reviews_month_dict[month].append({
                    'review_id': review_json['review_id'],
                    'stars': review_json['stars'],
                    'text': review_json['text'],
                    'date': review_json['date'],
                })
            if i > 0 and i % print_freq == 0:
                print(f'Finish reading {i} lines')
    print('Finish reading')

    for month, reviews in reviews_month_dict.items():
        df = pd.DataFrame(reviews)
        df.to_csv(f'reviews_{month}.csv', index=False)


if __name__ == '__main__':
    partition_dataset_stat()
    partition_dataset_by_month()
