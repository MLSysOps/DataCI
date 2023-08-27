#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Aug 28, 2023
"""
import augly.text as textaugs

from dataci.models import Workflow
from dataci.plugins.decorators import stage


@stage()
def text_augmentation(df):
    aug_function = textaugs.SimulateTypos()
    df['text'] = aug_function(df['text'].tolist())
    return df


if __name__ == '__main__':
    # Obtain the pipeline from DataCI pipeline registry:
    print('Obtain the sentiment analysis pipeline from DataCI pipeline registry...')
    sentiment_analysis_pipeline = Workflow.get('sentiment_analysis@v1')
    print(sentiment_analysis_pipeline)

    # Modify the pipeline definition by replacing the old text_augmentation stage with the new one:
    print('Modify the pipeline definition by replacing the old text_augmentation stage with the new one...')
    print('Old text_augmentation stage:\n', sentiment_analysis_pipeline.stages['text_augmentation'].script)
    print('New text_augmentation stage:\n', text_augmentation.script)
    sentiment_analysis_pipeline.patch(text_augmentation=text_augmentation)

    # Test the new pipeline locally:
    print('Test the new pipeline locally...')
    sentiment_analysis_pipeline.test()

    # Publish the new pipeline to DataCI pipeline registry:
    print('Publish the new pipeline to DataCI pipeline registry...')
    sentiment_analysis_pipeline.publish()
    print(sentiment_analysis_pipeline)

    # Run the new pipeline with the latest dataset, nothing need to be changed in the pipeline code
    print('Run the new pipeline with the latest dataset...')
    run_id = sentiment_analysis_pipeline.run()
    print(f'Run the pipeline {sentiment_analysis_pipeline} with run_id: {run_id}')
    print(
        f'Visit the pipeline run dashboard at '
        f'http://localhost:8080/taskinstance/list/?_flt_3_dag_id={sentiment_analysis_pipeline.backend_id} \n'
        f'to see the pipeline run result.'
    )
