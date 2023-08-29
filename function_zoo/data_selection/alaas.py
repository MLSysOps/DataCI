#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Aug 29, 2023
"""
import logging
import os

import pandas as pd
import torch
from alaas.server.executors import TorchALWorker
from docarray import Document, DocumentArray
from transformers import pipeline, AutoTokenizer

from dataci.plugins.decorators import stage

logger = logging.getLogger(__name__)

os.environ['KMP_DUPLICATE_LIB_OK'] = 'True'


@stage
def data_selection(
        df,
        num_samples: int,
        model_name='bert-base-uncased',
        strategy: str = 'RandomSampling',
        device: str = None
):
    text_list = df['text'].tolist()
    device = device or 'cuda' if torch.cuda.is_available() else 'cpu'

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
