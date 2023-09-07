#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Aug 22, 2023
"""
import augly.text as textaugs

from dataci.plugins.decorators import stage as task


@task
def text_augmentation(df):
    aug_function = textaugs.ReplaceSimilarUnicodeChars()
    df['text'] = aug_function(df['text'].tolist())
    return df


from datetime import datetime
from dataci.models import Event
from dataci.plugins.decorators import dag, Dataset


@dag(
    start_date=datetime(2020, 7, 30), schedule=None,
    trigger=[Event('publish', 'yelp_review@*', producer_type='dataset', status='success')],
)
def text_process_ci():
    # Instead of manually track the dataset version:
    #     raw_dataset_train = df.read_csv(f'yelp_review_{DATASET_VERSION}/train.csv')
    # input dataset versioning is automatically handled by DataCI
    raw_dataset_train = Dataset.get('yelp_review@latest')
    text_aug_df = text_augmentation(raw_dataset_train)

    # Instead of manually track the output dataset version:
    #    text_aug_df.to_csv(f'text_aug_{DATASET_VERSION}/train.csv')
    # output dataset versioning is automatically handled by DataCI.
    Dataset(name='text_aug', dataset_files=text_aug_df)


# Build the pipeline
text_process_ci_pipeline = text_process_ci()

if __name__ == '__main__':
    import time

    from dataci.models import Dataset

    # Prepare the input dataset, you can either provide a list or a file path
    print('Prepare the input streaming dataset...')
    yelp_review_dataset = Dataset('yelp_review', dataset_files=[
        {'date': '2020-10-05 00:44:08', 'review_id': 'HWRpzNHPqjA4pxN5863QUA', 'stars': 5.0,
         'text': "I called Anytime on Friday afternoon about the number pad lock on my front door. After several questions, the gentleman asked me if I had changed the battery.", },
        {'date': '2020-10-15 04:34:49', 'review_id': '01plHaNGM92IT0LLcHjovQ', 'stars': 5.0,
         'text': "Friend took me for lunch.  Ordered the Chicken Pecan Tart although it was like a piece quiche, was absolutely delicious!", },
        {'date': '2020-10-17 06:58:09', 'review_id': '7CDDSuzoxTr4H5N4lOi9zw', 'stars': 4.0,
         'text': "I love coming here for my fruit and vegetables. It is always fresh and a great variety. The bags of already diced veggies are a huge time saver.", },
    ]).publish(version_tag='2020-10')
    print(yelp_review_dataset)

    # Test the pipeline locally
    print('Test the pipeline locally...')
    text_process_ci_pipeline.test()
    # Publish the pipeline
    print('Publish the pipeline...')
    text_process_ci_pipeline.publish()
    # Run the pipeline on the server
    print('Manual trigger the pipeline run on the server...')
    run_id = text_process_ci_pipeline.run()
    print(f'Pipeline run id: {run_id}')
    print(
        f'Visit the pipeline run dashboard at '
        f'http://localhost:8080/taskinstance/list/?_flt_3_dag_id={text_process_ci_pipeline.backend_id} \n'
        f'to see the pipeline run result.'
    )

    print('Wait for the previous trigger pipeline run to finish...')
    time.sleep(15)
    print('Publish the new version of input streaming dataset...')
    # DataCI auto track the new version of yelp_review dataset
    yelp_review_dataset_v2 = Dataset('yelp_review', dataset_files=[
        {'date': '2020-11-02 00:59:29', 'review_id': 'LTr95e6eOmLc7S_1WxM88Q', 'stars': 5.0,
         'text': "First of all the owner and staff went above and beyond to make us feel comfortable and protected during covid.  Secondly, the bar had fantastic drinks", },
        {'date': '2020-11-16 22:07:48', 'review_id': 'UhA9H0LK59qZegWOxyotcA', 'stars': 5.0,
         'text': "Herbies is awesome! My brunch was perfect, service, food, and drinks! I will definitely continue coming back.", },
        {'data': '2020-11-18 06:38:46', 'review_id': 'YqTMxlbebNBDcKYTIUvrdw', 'stars': 5.0,
         'text': "You won't regret stopping here, hidden gem with great food and a laid back and comfortable atmosphere, a place you can gather with friends and they will treat you like family while you're there.", },
    ]).publish(version_tag='2020-11')
    print(yelp_review_dataset_v2)
    # Upon publishing of the new version of input dataset, the text process CI pipeline is automatically triggered
    # and run on the server.
    print(
        'Upon publishing of the new version of input dataset, '
        'the text process CI pipeline is automatically triggered and evaluate on '
        'the new version of input streaming dataset.'
    )
    print(
        f'Visit the pipeline run dashboard at '
        f'http://localhost:8080/taskinstance/list/?_flt_3_dag_id={text_process_ci_pipeline.backend_id} \n'
        f'to see the pipeline run result.'
    )
