In this tutorial, we are simulate a real-world case by using Yelp dataset and sending them to a pre-built
sentiment analysis pipeline in a streaming mode.

The scripts of this tutorial is available, you may run them at:
- [Before We Start](#0-before-we-start)
- [Section 1: Prepare the Yelp Dataset as Streaming Data](#1-prepare-the-yelp-dataset-as-streaming-data) [:scroll: code](./step1_prepare_yelp_dataset.py)
- [Section 2: Build a Sentiment Analysis Pipeline](#2-build-a-sentiment-analysis-pipeline) [:scroll: code](./step2_build_sentiment_analysis_pipeline.py)
- [Section 3: Simulate the Streaming Data Settings](#3-simulate-the-streaming-data-settings) [:scroll: code](./step3_simulate_streaming_data.py)
- [Section 4: Continuously improve the data pipeline](#4-continuously-improve-the-data-pipeline) [:scroll: code](./step4_continuously_improve_data_pipeline.py)

# 0. Before We Start

This tutorial use the following libraries, you need to manually installed:

- [AugLy](https://github.com/facebookresearch/AugLy)
- [PyTorch](https://pytorch.org/get-started/locally/)
- [Huggingface Transformers](https://huggingface.co/docs/transformers/installation)
- [ALaaS](https://github.com/MLSysOps/Active-Learning-as-a-Service)

After that, start DataCI server:

```shell
dataci start
```

# 1. Prepare the Yelp Dataset as Streaming Data

We are going to use the [Yelp Review Dataset](https://www.yelp.com/dataset) as the streaming data source.
We have processed the Yelp review dataset into a daily-based dataset by its `date`.
In this tutorial, we will only use the data from 2020-09-01 to 2020-11-30 to simulate the streaming data scenario.

Assume we are at end of October, we will use the latest available datasets as the training and validation 
datasets, respectively.
- `yelp_review_train_2020-10`: from 2020-09-01 to 2020-10-15
- `yelp_review_val_2020-10`: from 2020-10-16 to 2020-10-31

```python
from dataci.models import Dataset

DATA_URL_BASE = 'https://zenodo.org/record/8288433/files'

yelp_review_train = Dataset('yelp_review_train', dataset_files=f'{DATA_URL_BASE}/yelp_review_train_2020-10.csv').publish('2020-10')
yelp_review_val = Dataset('yelp_review_val', dataset_files=f'{DATA_URL_BASE}/yelp_review_val_2020-10.csv').publish('2020-10')
```

# 2. Build a Sentiment Analysis Pipeline

In this section, we will build a sentiment analysis pipeline by using the Yelp review dataset. The pipeline will
perform the following tasks:

1. Text augmentation: augment the text data by using the [AugLy](https://github.com/facebookresearch/AugLy)
2. Data selection: select the most informative data for training by using
   the [Active Learning as a Service (ALaaS)](https://github.com/MLSysOps/Active-Learning-as-a-Service)
3. Text classification training and offline evaluation: train a text classification model and evaluate it using the
   processed data.

## Stage 1: text augmentation

```python
import augly.text as textaugs
from dataci.plugins.decorators import stage


@stage
def text_augmentation(df):
    aug_function = textaugs.ReplaceSimilarUnicodeChars()
    df['text'] = aug_function(df['text'].tolist())
    return df
```

## Stage 2: data selection

We select the most informative data for training. DataCI provides a built-in data selection stage using
the [Active Learning as a Service (ALaaS)](https://github.com/MLSysOps/Active-Learning-as-a-Service) system, you can use it directly by importing from 
our data-centric function zoo:

```python
from dataci.function_zoo.data_selection.alaas import data_selection
```
You can always check the source code directory of the function by access its `script` attribute:
```python
import os

print(os.listdir(data_selection.script['path']))
```

By calling the `data_selection` function (later in pipeline define), it randomly select 5000 data from the input
dataset:
```python
@dag(...)
def sentiment_analysis():
    ...
    data_selection_df = data_selection(text_aug_dataset, num_samples=5000, strategy='RandomSampling')
    ...
```

## Stage 3: text classification training and offline evaluation

After we select the most informative data, we can train a text classification model and evaluate it using the processed
data.   
To simplify the process, DataCI have provided a built-in text classification training code, you can use it directly by
importing from our data-centric function zoo:

```python
from dataci.function_zoo.benchmark import train_text_classification
```

## Define the sentiment analysis pipeline

```python
from datetime import datetime

from dataci.plugins.decorators import dag, Dataset


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
```

We write the workflow process in a function `text_classification` in a normal way, and then decorate it with `@dag` to
convert it to a DataCI tracked workflow. The `@dag` decorator will automatically track the versions of the
workflow definition, its stages implementation and the input/output datasets.

Debug, publish, and run the pipeline remotely:

```python
if __name__ == '__main__':
    # Test the pipeline locally
    sentiment_analysis_pipeline.test()

    # Publish the pipeline, the pipeline and its stages will be versioned and tracked:
    #     - Stage text_augmentation@v1
    #     - Stage select_data@v1
    #     - Stage train_text_classification_model@v1
    #     - Pipeline sentiment_analysis@v1
    sentiment_analysis_pipeline.publish()

    # Run the pipeline on the server, with the latest version of yelp_review_train and yelp_review_val datasets
    run_id = sentiment_analysis_pipeline.run()
    print(f'Run the pipeline with run_id: {run_id}')
```

Go to [pipeline runs dashboard](http://localhost:8080/taskinstance/list/?_flt_3_dag_id=default--sentiment_analysis--v1)
to check the pipeline run result.

# 3. Simulate the Streaming Data Settings

In the real world, the data is not static, it is continuously generated. In this section, we will simulate the
streaming data scenario by sending a new batch of data to the pipeline.

Assume that one month later, we have another batch of review data from Yelp, we create a new training and validation
dataset by using the new data:
- `yelp_review_train_2020-11`: from 2020-10-01 to 2020-11-15
- `yelp_review_val_2020-11`: from 2020-11-16 to 2020-11-30

```python
from dataci.models import Dataset

DATA_URL_BASE = 'https://zenodo.org/record/8288433/files'

train_dataset = Dataset('yelp_review_train', dataset_files=f'{DATA_URL_BASE}/yelp_review_train_2020-11.csv').publish('2020-11')
val_dataset = Dataset('yelp_review_val', dataset_files=f'{DATA_URL_BASE}/yelp_review_val_2020-11.csv').publish('2020-11')
````

Run the pipeline with the new dataset:

```python
from dataci.models import Workflow

# Obtain the sentiment analysis pipeline v1 from DataCI pipeline registry:
sentiment_analysis_pipeline = Workflow.get('sentiment_analysis@v1')
# Run the pipeline with the new dataset, nothing need to be changed in the pipeline code
sentiment_analysis_pipeline.run()
```

Alternatively, we can let DataCI automatically trigger the pipeline run upon a new dataset is published,
please refer to the [DataCI Trigger Tutorial]() (WIP).

Go to [pipeline runs dashboard](http://localhost:8080/taskinstance/list/?_flt_3_dag_id=default--sentiment_analysis--v1)
to check the pipeline run result.

# 4. Continuously improve the data pipeline

As the time goes, we want to improve the data pipeline by try new data augmentation methods and new data selection
methods. In this section, we will modify the existing data pipeline by using a new text augmentation method. Then we
will perform this experiment and get the evaluation results easily by DataCI.

We write text augmentation v2:

```python
import augly.text as textaugs
from dataci.plugins.decorators import stage


@stage()
def text_augmentation(df):
    aug_function = textaugs.SimulateTypos()
    df['text'] = aug_function(df['text'].tolist())
    return df
```

Modify the pipeline definition to use the new method:

```python
from dataci.models import Workflow

# Obtain the pipeline from DataCI pipeline registry:
sentiment_analysis_pipeline = Workflow.get('sentiment_analysis@v1')
# Modify the pipeline definition by replacing the old text_augmentation stage with the new one:
sentiment_analysis_pipeline.patch(text_augmentation=text_augmentation)
# Test the new pipeline locally:
sentiment_analysis_pipeline.test()
# Publish the new pipeline to DataCI pipeline registry:
sentiment_analysis_pipeline.publish()
```

Run the new pipeline remotely using the latest version of the datasets:

```python
sentiment_analysis_pipeline.run()
```
