# Data CI

A tool to record data-science triplets (🗂️ `dataset`, 📏 `pipeline`, 📊 `results`)

- 🗂️ Versioning every data and its code with accumulated human data-pipeline-building knowledge
- 📏 Easy pipeline debugging with interatives
- 📊 One-click data-centric benchmarking with comprehensive dataset report

## Installation

### PyPI install

WIP

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

With few lines of modification, you can easily track every version of the input, output data and code for your existing
data pipeline.

1. Write a data augmentation stage in `text_augmentation.py`:

```python
import augly.text as textaugs
from dataci.plugins.decorators import stage as task


@task
def text_augmentation(df):
    aug_function = textaugs.ReplaceSimilarUnicodeChars()
    df['text'] = aug_function(df['text'].tolist())
    return df
```

2. Define the text processing CI pipeline in `text_classification.py`:

```python
from datetime import datetime

from dataci.models import Event
from dataci.plugins.decorators import dag, Dataset

from text_augmentation import text_augmentation


@dag(
    start_date=datetime(2020, 7, 30), schedule_interval=None,
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
    Dataset(name='text_aug', dataset_files=text_aug_df, file_reader=None)


text_process_ci_pipeline = text_process_ci()
```

3. Manually run the pipeline:

```python
from dataci.models import Dataset

# Prepare the input dataset
yelp_review_dataset = Dataset('yelp_review', dataset_files='yelp_review_v1/train.csv').publish(version_tag='v1')
# Publish the pipeline
text_process_ci_pipeline.publish()
# Run the pipeline
text_process_ci_pipeline.run()
```

The text process CI pipeline will be triggered and run. You can see the real-time logging on the server console.

4. Automatically trigger the pipeline by publish a new version of input dataset:

```python
from dataci.models import Dataset

# DataCI auto track the new input dataset version
yelp_review_dataset = Dataset('yelp_review', dataset_files='yelp_review_v2/train.csv')
yelp_review_dataset.publish(version_tag='v2')
```

Upon the publish of the new version of input dataset, the text process CI pipeline is automatically triggered and run.
You can see the real-time logging on the server console.

## More Examples

- [Play with the CI](./example/ci/README.md)
- [Build Text Classification Dataset](./docs/Create_Text_Classification_Dataset.md) [Jupyter Notebooks](./docs/Create_Text_Classification_Dataset.ipynb)
- [Data-centric Benchmark: Compare Different Dataset Versions](./docs/Data-centric_Benchmark.md)
- [Data Analysis for Performance Checking and Debugging](./docs/Data_Analysis.md)

## Citation

Our tech report is available at [arXiv:2306.15538](https://arxiv.org/abs/2306.15538)
and [DMLR@ICML'23](https://dmlr.ai/assets/accepted-papers/42/CameraReady/DataCI_v3_camera_ready.pdf). Please cite as:

```bibtex
@article{zhang2023dataci,
    title = {DataCI: A Platform for Data-Centric AI on Streaming Data},
    author = {Zhang, Huaizheng and Huang, Yizheng and Li, Yuanming},
    journal = {arXiv preprint arXiv:2306.15538},
    year = {2023}
}
```

## Contributors

## License
