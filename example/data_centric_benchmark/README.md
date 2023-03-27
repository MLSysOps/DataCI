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
text_classification_datasets = list_dataset('train_data_pipeline:text_aug', tree_view=False)
# Sort by created date
text_classification_datasets.sort(key=lambda x: x.create_date)
train_dataset = text_classification_datasets[0]
```

Get validation split of the raw text dataset v1 as test dataset

```python
from dataci.dataset import list_dataset

# Get all versions of the raw text dataset val split
text_raw_val_datasets = list_dataset('text_raw_val', tree_view=False)
# Sort by created date
text_raw_val_datasets.sort(key=lambda x: x.create_date)
test_dataset = text_raw_val_datasets[0]
```

Since the text classification dataset v1 are built with the data augmentation pipeline `train_data_pipeline`,
we will perform `data_augmentation` data-centric benchmark, with `text_classification` ML task.

We will use `bert-base-cased` as the model name, and only train for 3 epochs with 10 steps per epoch for demo purpose.
```python
from dataci.benchmark import Benchmark

# Train dataset is the text classification dataset v1
train_dataset = ...
# Test dataset is the raw text dataset v1 val split
test_dataset = ...

benchmark = Benchmark(
    type='data_augmentation',
    ml_task='text_classification',
    model_name='bert-base-cased',
    train_dataset=train_dataset,
    test_dataset=test_dataset,
    train_kwargs=dict(
        epochs=3,
        batch_size=4,
        learning_rate=1e-5,
        logging_steps=1,
        max_train_steps_per_epoch=10,
        max_val_steps_per_epoch=10,
        seed=42,
    ),
)
# Run benchmark
benchmark.run()

# Check benchmark results
print(benchmark.metrics)
```

## 1.2 Benchmark all text classification datasets (v2 - v4)

```python

for text_classification_dataset in text_classification_datasets[1:]:
    benchmark = Benchmark(
        type='data_augmentation',
        ml_task='text_classification',
        model_name='bert-base-cased',
        train_dataset=text_classification_dataset,
        test_dataset=test_dataset,
        train_kwargs=dict(
            epochs=3,
            batch_size=4,
            learning_rate=1e-5,
            logging_steps=1,
            max_train_steps_per_epoch=10,
            max_val_steps_per_epoch=10,
            seed=42,
        ),
    )
    # Run benchmark
    benchmark.run()
```

# 2. Summary

## 2.1 What is the best dataset for text classification?

```shell
python dataci/command/benchmark.py ls -me=val/loss,val/acc,test/acc,test/batch_time train_data_pipeline:text_aug
# Train dataset: train_data_pipeline:text_aug, Test dataset: text_raw_val@641a430
# Type: data_augmentation, ML task: text_classification, Model name: bert-base-cased
# Dataset version Val loss   Val acc    Test acc   Test batch_time
# 9052571           0.741362   0.778219   0.741667   0.011514
# 05fdad4           1.030018   0.598159   0.710417   0.011522
# 7720705           0.938506   0.641615   0.760938   0.011533
# 085beda           0.706578   0.756588   0.751563   0.011555
# 
# Total 1 benchmark, 4 records
```

## 2.2 What is the best data augmentation pipeline for text classification?

```shell
python dataci/command/benchmark.py lsp -me=val/acc,test/acc train_data_pipeline
# Type: data_augmentation, ML task: text_classification, Model name: bert-base-cased
# Pipeline                       Dataset                                                                         
#                                text_raw_train@f3a821d                    text_raw_train@09d026c
#                                Val acc              Test acc             Val acc              Test acc
# train_data_pipeline@6a552e1    0.598159             0.710417             0.641615             0.760938
# train_data_pipeline@df63fb1    0.778219             0.741667             0.756588             0.751563
# 
# Total 1 benchmark, 4 records, 2 pipelines
```
