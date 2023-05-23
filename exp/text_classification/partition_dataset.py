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
OUTPUT_DIR = Path('data')
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


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


def split_train_val(yr_quarter='2020Q4'):
    year, month = yr_quarter[:4], int(yr_quarter[5:]) * 3
    # We leave last half month of the quarter for offline evaluation
    df = pd.read_csv(OUTPUT_DIR / f'reviews_{year}{month:02}.csv')
    # Select the last half month of the quarter as training set
    print(f'Date after {year}-{month:02}-16 00:00:00 will be in the validation set')
    df_val = df[df['date'] >= f'{year}-{month:02}-16 00:00:00']

    # Use the first 2 + 1/2 months of this quarter as training set
    df_train = []
    for i in (-2, -1):
        df_train.append(pd.read_csv(OUTPUT_DIR / f'reviews_{year}{month + i}.csv'))
    df_train.append(df[df['date'] < f'{year}-{month:02}-16 00:00:00'])
    df_train = pd.concat(df_train)
    print(len(df_train), len(df_val))

    # Save to file
    df_train.to_csv(OUTPUT_DIR / f'reviews_{yr_quarter}_train.csv', index=False)
    df_val.to_csv(OUTPUT_DIR / f'reviews_{yr_quarter}_val.csv', index=False)


if __name__ == '__main__':
    # partition_dataset_stat()
    # partition_dataset_by_month()
    split_train_val()
