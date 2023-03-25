In the previous tutorial, we showed how DataCI helps manage and build datasets with different raw datasets and
pipelines. In this tutorial, we will show how to use DataCI to benchmark the dataset.

Data is the most important part of the machine learning pipeline. Data scientists spend most of their time cleaning,
augmenting, and preprocessing data, only to find the best online performance with the same model structure.
[In the previous tutorial](/example/create_text_classification_dataset), we built 4 versions of the text classification
dataset `train_data_pipeline:text_aug`. We are now going to determine which dataset performs the best.

# 0. Prerequisites

Since we are using datasets built in the previous tutorial, please make sure you have run the previous tutorial.

## Publish text classification dataset v1 - v4

We have published 4 versions of the text classification dataset in the previous tutorial.
We can list them with the following command:

```shell
python dataci/command/dataset.py ls train_data_pipeline:text_aug
```

# 1. Benchmark Text Classification Dataset

Recall that in the previous tutorial, we have benchmark the performance of the text classification dataset v1 by
a training script. We can do so easily with DataCI's data-centric benchmark tool.

## 1.1 Benchmark text classification dataset v1

Get text classification dataset v1 as train dataset

```python
from dataci.dataset import list_dataset

# Get all versions of the text classification dataset
text_classification_datasets = list_dataset('text_classification')
# Sort by created date
text_classification_datasets.sort(key=lambda x: x.create_date)
train_dataset = text_classification_datasets[0]
```

Get validation split of the raw text dataset v1 as test dataset

```python
from dataci.dataset import list_dataset

# Get all versions of the raw text dataset val split
text_raw_val_datasets = list_dataset('text_raw_val')
# Sort by created date
text_raw_val_datasets.sort(key=lambda x: x.create_date)
test_dataset = text_raw_val_datasets[0]
```

Since the text classification dataset v1 are built with the data augmentation pipeline `train_data_pipeline`,
we will perform `data_augmentation` data-centric benchmark, with `text_classification` ML task.

```python
from dataci.benchmark import Benchmark

# Train dataset is the text classification dataset v1
train_dataset = ...
# Test dataset is the raw text dataset v1 val split
test_dataset = ...

benchmark = Benchmark(
    type='data_augmentation',
    ml_task='text_classification',
    model='bert-base-cased',
    train_dataset=train_dataset,
    test_dataset=test_dataset,
)
# Run benchmark
benchmark.run()
```

## 1.2 Benchmark all text classification datasets (v2 - v4)

```shell
python dataci/command/benchmark.py run --type=data_augmentation --ml_task=text_classification --model=bert-base-cased --train_dataset=text_classification@v2 --test_dataset=text_raw_val@v1

python dataci/command/benchmark.py run --type=data_augmentation --ml_task=text_classification --model=bert-base-cased --train_dataset=text_classification@v3 --test_dataset=text_raw_val@v2

python dataci/command/benchmark.py run --type=data_augmentation --ml_task=text_classification --model=bert-base-cased --train_dataset=text_classification@v4 --test_dataset=text_raw_val@v2
```

# 2. Summary

## 2.1 What is the best dataset for text classification?

```shell
dataci benchmark ls -desc=val/auc -n text_classification

Data Augmentation + Text Classification + Finetune + SentenceBERT
ID      dataset ver     train/auc   train/acc   val/auc     val/acc
bench3  v3              0.88        0.90        0.75        0.80              
bench2  v2              0.82        0.85        0.70        0.78
bench1  v1              0.75        0.86        0.70        0.72

Total 1 benchmark, 3 records
```

## 2.2 What is the best data augmentation pipeline for text classification?
