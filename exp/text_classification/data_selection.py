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
import os
from pathlib import Path

import pandas as pd
import torch
from alaas.server.executors import TorchALWorker
from docarray import Document, DocumentArray
from transformers import pipeline, AutoTokenizer

from dataci.plugins.decorators import stage

logger = logging.getLogger(__name__)

os.environ['KMP_DUPLICATE_LIB_OK'] = 'True'


@stage
def select_data(df, num_samples: int, model_name, strategy: str = 'RandomSampling', device: str = None):
    text_list = df['text'].tolist()
    device = device or 'cuda' if torch.cuda.is_available() else 'cpu'

    # Large number of data breaks the server, let's do in a non-server-client way

    # Start ALaaS server in a separate process
    logger.info('Start AL Worker')
    al_worker = TorchALWorker(
        model_name=model_name,
        model_repo="huggingface/pytorch-transformers",
        device=device,
        strategy=strategy,
        minibatch_size=1024,
        tokenizer_model="bert-base-uncased",
        task="text-classification",
    )
    # Monkey patch the model
    tokenizer = AutoTokenizer.from_pretrained(al_worker._tokenizer_model)
    al_worker._model = pipeline(
        al_worker._task,
        model=al_worker._model_name,
        tokenizer=tokenizer,
        device=al_worker._convert_torch_device(),
        padding=True, truncation=True, max_length=256, return_all_scores=True
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


if __name__ == '__main__':
    exp_time = '2021Q4'
    prefix = ''
    DATASET_BASE_PATH = prefix + 'data/reviews_{}_train.csv'
    MODEL_NAME = prefix + 'out/2021Q3_LC_Token_lr=1.00E-05_b=128_j=4/model/'
    SAVE_DATASET_BASE_PATH = prefix + 'processed/'

    cur_year, cur_quarter = exp_time.split('Q')
    last_quarter = f'{int(cur_year) - 1}Q4' if cur_quarter == '1' else f'{cur_year}Q{int(cur_quarter) - 1}'
    data_selection_method = 'LC'
    strategy_name_mapper = {
        'RS': 'RandomSampling',
        'LC': 'LeastConfidence',
    }
    save_path = SAVE_DATASET_BASE_PATH + f'data_select_{exp_time}_{data_selection_method}.csv'
    Path(save_path).parent.mkdir(parents=True, exist_ok=True)

    # Read input data
    df_ = pd.read_csv(DATASET_BASE_PATH.format(exp_time))

    # Run stage: select_data
    selected_df = select_data.test(
        df_, num_samples=5000, model_name=MODEL_NAME, strategy=strategy_name_mapper[data_selection_method]
    )

    # Save output data
    selected_df.to_csv(save_path, index=False)
