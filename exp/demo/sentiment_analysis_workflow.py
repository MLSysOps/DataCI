#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: May 30, 2023
"""
from dataci.decorators.stage import stage
from dataci.models import Workflow


@stage()
def data_selection(df):
    import pandas as pd
    from alaas.server.executors import TorchALWorker
    from docarray import Document, DocumentArray

    num_samples = 50_000
    # Load data
    text_list = df['text'].tolist()

    # Large number of data breaks the server, let's do in a non-server-client way

    # Start ALaaS server in a separate process
    print('Start AL Worker')
    al_worker = TorchALWorker(
        model_name='bert-base-uncased',
        model_repo="huggingface/pytorch-transformers",
        device='cuda',
        strategy='RandomSampling',
        minibatch_size=1024,
        tokenizer_model="bert-base-uncased",
        task="text-classification",
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
    print('Get selected rows by selected text')
    selected_df = df.merge(query_df, on='text', how='inner')

    return selected_df


@stage()
def text_augmentation(df):
    # Skip augmentation
    return df


@stage()
def model_training(df):
    print('Dummy model training')
    return None


@stage()
def offline_evaluation(model):
    print('Dummy offline evaluation')


sentiment_analysis_workflow = Workflow('sentiment_analysis')
with sentiment_analysis_workflow:
    data_selection >> text_augmentation >> model_training >> offline_evaluation
