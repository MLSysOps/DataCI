import logging
import os
from datetime import datetime

import augly.text as textaugs

from dataci.plugins.decorators import stage, dag, Dataset
from dataci.function_zoo.data_selection.alaas import data_selection
from dataci.function_zoo.benchmark import train_text_classification

logger = logging.getLogger(__name__)

os.environ['KMP_DUPLICATE_LIB_OK'] = 'True'


@stage
def text_augmentation(df):
    aug_function = textaugs.ReplaceSimilarUnicodeChars()
    df['text'] = aug_function(df['text'].tolist())
    return df


@dag(
    start_date=datetime(2020, 7, 30), schedule=None,  # purely by trigger
)
def sentiment_analysis():
    raw_dataset_train = Dataset.get('yelp_review_train@latest')
    raw_dataset_val = Dataset.get('yelp_review_val@latest', file_reader=None)

    text_aug_df = text_augmentation(raw_dataset_train)
    text_aug_dataset = Dataset(name='text_aug', dataset_files=text_aug_df)
    data_selection_df = data_selection(text_aug_dataset, num_samples=5000, strategy='RandomSampling')
    data_select_dataset = Dataset(name='data_selection', dataset_files=data_selection_df, file_reader=None)
    train_outputs = train_text_classification(train_dataset=data_select_dataset, test_dataset=raw_dataset_val)


sentiment_analysis_pipeline = sentiment_analysis()

if __name__ == '__main__':
    # Test the pipeline locally
    print(f'Test {sentiment_analysis_pipeline} locally')
    # sentiment_analysis_pipeline.test()

    # Publish the pipeline, the pipeline and its stages will be versioned and tracked:
    #     - Stage text_augmentation@v1
    #     - Stage select_data@v1
    #     - Stage train_text_classification_model@v1
    #     - Pipeline sentiment_analysis@v1
    print(f'Publish {sentiment_analysis_pipeline}')
    sentiment_analysis_pipeline.publish()

    # Run the pipeline on the server, with the latest version of yelp_review_train and yelp_review_val datasets
    run_id = sentiment_analysis_pipeline.run()
    print(f'Run the pipeline {sentiment_analysis_pipeline} with run_id: {run_id}')
    print(
        f'Visit the pipeline run dashboard at '
        f'http://localhost:8080/taskinstance/list/?_flt_3_dag_id={sentiment_analysis_pipeline.backend_id} \n'
        f'to see the pipeline run result.'
    )
