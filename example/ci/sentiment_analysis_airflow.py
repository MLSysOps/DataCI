#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: May 30, 2023
"""
from datetime import datetime

from dataci.decorators import stage as task
from dataci.orchestrator.airflow import DAG


@task
def read_dataset():
    import pandas as pd

    df = pd.read_csv('../../exp/text_classification/data/reviews_2020Q4_train.csv')
    return df


@task
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


@task
def text_augmentation(df):
    # Skip augmentation
    return df


@task
def model_training(df):
    print('Dummy model training')
    return None


@task
def offline_evaluation(model):
    print('Dummy offline evaluation')


# @dag(
#     start_date=datetime(2020, 1, 1),
# )
# def sentiment_analysis():
with DAG(
        dag_id='sentiment_analysis',
        start_date=datetime(2020, 1, 1),
        schedule_interval=None,
        catchup=False,
) as dag:
    df = read_dataset()
    df = data_selection(df)
    df = text_augmentation(df)
    model = model_training(df)
    offline_evaluation(model)

if __name__ == '__main__':
    # sentiment_analysis_workflow.publish()
    # print(read_dataset.function)
    # dag = sentiment_analysis()
    print(dag)
    # dag.test()
