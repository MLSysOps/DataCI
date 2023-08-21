# Data CI

A tool to record data-science triplets (🗂️ `dataset`, 📏 `pipeline`, 📊 `results`)

- 🗂️ Versioning every data and code with accumulated human data-pipeline-building knowledge
- 📏 Easy pipeline debugging with interatives
- 📊 One-click data-centric benchmarking with comprehensive dataset report

## Installation

### PyPI install

### Manual install

```shell
pip install -e .
```

Start DataCI server
```shell
dataci init  # For the first time, initialize the project
dataci server start
```

## Quick Start

With few lines of modification, you can easily enable the data and code versioning and CI for your existing data pipeline.

1. Build a text processing stage at `text_augmentation.py`:

```diff
import augly.text as textaugs

+from dataci.plugins.decorators import stage as task


+@task
def text_augmentation(df):
    aug_function = textaugs.ReplaceSimilarUnicodeChars()
    df['text'] = aug_function(df['text'].tolist())
    return df
```

2. Define the text classification pipeline with pre-defined CI:

```diff
from datetime import datetime

from dataci.models import Event
+from dataci.plugins.decorators import dag, Dataset
from function_zoo.benchmark import train_text_classification

from text_augmentation import text_augmentation


# previously, you need to manually track the input dataset version
-DATASET_VERSION = 'v1'

+@dag(
+    start_date=datetime(2020, 7, 30), schedule_interval=None,
+    trigger=[Event('publish', 'yelp_review@*', producer_type='dataset', status='success')],
+)
def text_classification():
    # input dataset versioning is automatically handled by DataCI
    -raw_dataset_train = df.read_csv(f'yelp_review_{DATASET_VERSION}/train.csv')
    raw_dataset_train = Dataset.get('yelp_review@latest')
    text_aug_df = text_augmentation(raw_dataset_train)
    # output dataset versioning is automatically handled by DataCI.
    -text_aug_dataset = text_aug_df.to_csv(f'text_aug_{DATASET_VERSION}/train.csv')
    +text_aug_dataset = Dataset(name='text_aug', dataset_files=text_aug_df, file_reader=None)
    # Use the pre-defined CI stages from function zoo
    output_path = train_text_classification(train_dataset=text_aug_dataset)


text_classification_pipeline = text_classification()

if __name__ == '__main__':
    # Publish the dataset for testing
    +Dataset(name='yelp_review', dataset_files='yelp_review_v1/train.csv').publish(version_tag='v1')
    # Test the pipeline
    -text_classification_pipeline()
    +text_classification_pipeline.test()
    # Publish the pipeline, pipeline versioning is automatically tracked by DataCI
    text_classification_pipeline.publish()
```

3. DataCI will automatically run your pipeline upon publish of a new version of input dataset:
```python
from dataci.models import Dataset

# DataCI auto track the new input dataset version
yelp_review_dataset = Dataset('yelp_review', dataset_files='yelp_review_v2/train.csv')
# Upon publish of the new version of input dataset, the text_classification_pipeline is automatically trigger
yelp_review_dataset.publish(version_tag='v2')
```

You can view the triggered pipeline run result in the UI:
[![Pipeline Run Result]](http://localhost:8080/)


## More Examples

- [Play with the CI](./example/ci/README.md)
- [Build Text Classification Dataset](./docs/Create_Text_Classification_Dataset.md) [Jupyter Notebooks](./docs/Create_Text_Classification_Dataset.ipynb)
- [Data-centric Benchmark: Compare Different Dataset Versions](./docs/Data-centric_Benchmark.md)
- [Data Analysis for Performance Checking and Debugging](./docs/Data_Analysis.md)

## Contributors

## License
