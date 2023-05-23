#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: May, 23, 2023

1. Download dataset from Google Drive
2. Unzip dataset to current directory / dataset base directory
"""
import logging
from datetime import datetime
from pathlib import Path

import pandas as pd
from alaas.server.executors import TorchALWorker
from docarray import Document, DocumentArray

DATASET_BASE_PATH = 'data/reviews_{}_train.csv'

logger = logging.getLogger(__name__)


def read_dataset(exp_time):
    df = pd.read_csv(DATASET_BASE_PATH.format(exp_time))
    return df


def select_data(df, num_samples: int, strategy: str = 'RandomSampling'):
    text_list = df['text'].tolist()

    # Large number of data breaks the server, let's do in a non-server-client way

    # Start ALaaS server in a separate process
    logger.info('Start AL Worker')
    al_worker = TorchALWorker(
        model_name="bert-base-uncased",
        model_repo="huggingface/pytorch-transformers",
        device='cuda',
        strategy=strategy,
        minibatch_size=8,
        tokenizer_model="bert-base-uncased",
        task="text-classification"
    )

    # Prepare data for ALWorker
    doc_list = []
    for txt in text_list:
        doc_list.append(Document(text=txt, mime_type='text'))

    queries = al_worker.query(
        DocumentArray(doc_list),
        parameters={'budget': num_samples, 'n_drop': None}
    ).to_list()
    query_df = pd.DataFrame(queries, columns=['text'])

    # Get back selected rows
    logger.info('Get selected rows by selected text')
    selected_df = df.merge(query_df, on='text', how='inner')

    return selected_df


def save_dataset(df, file_path):
    Path(file_path).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(file_path, index=False)


if __name__ == '__main__':
    exp_time = '2021Q1'
    cur_year, cur_quarter = exp_time.split('Q')
    last_quarter = f'{int(cur_year) - 1}Q4' if cur_quarter == '1' else f'{cur_year}Q{int(cur_quarter) - 1}'
    data_start_date = '202010'
    data_end_date = '202012'
    data_selection_method = 'RS'
    strategy_name_mapper = {
        'RS': 'RandomSampling',
    }

    df_ = read_dataset(last_quarter)
    selected_df = select_data(df_, num_samples=50_000, strategy=strategy_name_mapper[data_selection_method])
    save_dataset(selected_df, f'processed/data_select_{exp_time}_{data_selection_method}.csv')
