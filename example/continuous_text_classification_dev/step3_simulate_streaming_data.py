#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Aug 28, 2023
"""
from dataci.models import Dataset, Workflow

print('Publishing new dataset version...')
DATA_URL_BASE = 'https://zenodo.org/record/8288433/files'

yelp_review_train_v2 = Dataset('yelp_review_train', dataset_files=f'{DATA_URL_BASE}/yelp_review_train_2020-11.csv').publish('2020-11')
yelp_review_val_v2 = Dataset('yelp_review_val', dataset_files=f'{DATA_URL_BASE}/yelp_review_val_2020-11.csv').publish('2020-11')
print(yelp_review_train_v2, yelp_review_val_v2)

# Obtain the sentiment analysis pipeline v1 from DataCI pipeline registry:
sentiment_analysis_pipeline = Workflow.get('sentiment_analysis@v1')
# Run the pipeline with the new dataset, nothing need to be changed in the pipeline code
run_id = sentiment_analysis_pipeline.run()
print(f'Run the pipeline {sentiment_analysis_pipeline} with run_id: {run_id}')
print(
    f'Visit the pipeline run dashboard at '
    f'http://localhost:8080/taskinstance/list/?_flt_3_dag_id={sentiment_analysis_pipeline.backend_id} \n'
    f'to see the pipeline run result.'
)
