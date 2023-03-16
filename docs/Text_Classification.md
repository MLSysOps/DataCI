Given a product title, we are going to determine the product cateogry.
# 0. Prerequisites

## Initialize DataCI

```shell
python dataci/command/init.py
```

## Download Sample Raw Data

For this tutorial, we download sampled product data provided by our e-commerce partners. This data
is collected from internal online data lake with proper removal of confidential information.

```shell
# saved at data/pairwise_raw/
mkdir -p data
rm -r data/*
cp -r dataset/multimodal_pairwise_v1 data/pairwise_raw/
```

This dataset contains train and val splits. Each split contains a CSV file with 3 columns:
`product_id`, `product_title` and `lv3_category`. We are going to build a pipeline to classify the product category
(`lv3_category`) based on its raw, dirty `product_title`.

# 1. Build Text Classification Dataset

## 1.1 Publish raw data

Add this dataset into the data repository.

```shell
python dataci/command/dataset.py publish -n pairwise_raw_train data/pairwise_raw/train.csv
```

## 1.2 Build a dataset for text classification 

1. Build train dataset v1

```python
import augly.text as txtaugs
import os
import pandas as pd

from dataci.pipeline import Pipeline, stage


@stage(inputs='pairwise_raw_train', outputs='text_clean.csv')
def text_clean(inputs):
    df = pd.read_csv(os.path.join(inputs, 'train.csv'))
    df['to_product_name'] = df['to_product_name'].map(lambda text: text.lower())
    return df


@stage(inputs='text_clean.csv', outputs='text_aug.csv')
def text_augmentation(inputs):
    df = pd.read_csv(inputs)
    transform = txtaugs.InsertPunctuationChars(
        granularity="all",
        cadence=5.0,
        vary_chars=True,
    )
    df['to_product_name'] = df['to_product_name'].map(transform)
    return df


train_data_pipeline = Pipeline(name='train_data_pipeline', stages=[text_clean, text_augmentation])
```

Debug/test run the train data pipeline:
```python
train_data_pipe_run = train_data_pipeline()
```

The output `text_augmentation.csv` will be used as train dataset.

2. Run training with the built train dataset v1
   Now you can simple train a pre-trained BERT on this text classification dataset v1:
```shell
python train.py --dataset ./train_data_pipeline/text_aug.csv
```

4. Save data pipeline

You can now publish your data pipeline for a better management.

```python
train_data_pipeline.publish()
```

## 1.3 Publish first version of text dataset

Run the published pipeline `train_text_classification`, its final output `text_aug.csv` will be
automatically published as a dataset: `train_text_classification:text_aug`.

```python
train_data_pipe_run()
```

# 2. Try with New Data Augmentation Method

Let's create a second version of `train_text_classification:text_aug` for text classification with
different data augmentation method to improve the model performance.

## 2.1 Write a second version train data pipeline

We design a better data augmentation method for `train_data_pipeline_v2`:

```python
import unicodedata
from cleantext import clean


def clean_func(text):
    # remove emoji, space, and to lower case
    text = clean(text, to_ascii=False, lower=True, normalize_whitespace=True, no_emoji=True)
    # remove accent
    text = ''.join(c for c in unicodedata.normalize('NFD', text) if unicodedata.category(c) != 'Mn')
    return text


@stage(inputs='pairwise_raw[val]', outputs='text_clean.csv')
def text_clean(inputs):
    df = pd.read_csv(os.path.join(inputs, 'train.csv'))
    df['to_product_name'] = df['to_product_name'].map(lambda text: text.lower())
    return df


train_data_pipeline_v2 = Pipeline([text_clean, text_augmentation])
```

## 2.2 Publish train data pipeline v2
```python
train_data_pipeline_v2.publish(name='train_data_pipeline')
```

Now, let's check our pipeline `train_data_pipeline`:
```shell
dataci pipeline ls -n train_data_pipeline
# train_data_pipeline
# | - v1
# |    | - run1
# | - v2
```

## 2.3 Publish text classification dataset v2
It is easy to update output dataset once our data pipeline have new version:

```python
train_data_pipeline_v2()
# [pairwise_raw@v1] >>> train_data_pipeline@v2.run1 >>> [text_classification@v2]
```
You can also trigger the version update from the dataset side:
```shell
dataci dataset update -n text_classification
# Searching changes...
# - pairwise_raw -
# - train_data_pipeline@v1 -> train_data_pipeline@v2
# - val_data_pipeline -
# Found new verion of output pipeline: train_data_pipeline@v2
# Trigger dataset update
# [D] pairwise_raw@v1[train] >>> train_data_pipeline@v2.run1 >>> [D] text_classification@v2[train]
# Finish 1/1!
```

# 3. Try with more raw data
Assume our parterner hand over more raw data to us, and we are working on the new dataset:
```shell
# Download pairwise_raw_v2
```

Publish raw data v2:
```shell
dataci dataset publish -n pairwise_raw data/pairwise_raw_v2
```

Recall how we trigger an update to our text classification dataset:
```shell
dataci dataset update -n text_classification --all
# Searching changes...
# - pairwise_raw@v1 -> pairwise_raw@v2
# - train_data_pipeline -
# - val_data_pipeline -
# Found new verion of parent dataset: pairwise_raw@v2
# Trigger dataset update
# [D] pairwise_raw@v2[train] >>> train_data_pipeline@v2.run2 >>> [D] text_classification@v3[train]
# Finish 1/1!
# Run with latest data pipeline version only by default. 
# To run all pipeline versions, please add `--all`.
```

# 4. Summary
That is a long journey! Wait, how many dataset we have and what are their performance?
It seems quite messy after we publish many datasets and pipelines, run a lot of workflows 
and benchmarks.  
Lickly, when we developing our data pipelines, DataCI helps in managing and auditing all of them!

## 4.1 How many datasets and their relationship?

1. Check all registered dataset
```shell
dataci dataset ls -a

pairwise_raw
|  [train] <<< Manual Upload
|  version  split       output ver.     parent dataset ver. size    update time
| - v2      train       N.A.            N.A.                40M     2023-02-02 19:00:00
| - v1      train       N.A.            N.A.                20M     2023-02-01 19:00:00
|
|  [val] <<< Manual Upload
|  version  split       output ver.     parent dataset ver. size    update time
| - v1      val         N.A.            N.A.                2.0M    2023-02-01 19:00:00
Total 2 split, 2 version

text_classification
|  train << train_data_pipeline << pairwise_raw[train]
|  version  split       output pipeline ver     parent dataset ver  size    update time
| - v3      train       v2.run2                 v2[train]           200K    2023-02-02 19:00:00
| - v2      train       v2.run1                 v1[train]           100K    2023-02-01 19:00:00
| - v1      train       v1.run1                 v1[train]           100K    2023-02-01 19:00:00
|
|  val <<< val_data_pipeline <<< pairwise_raw[val]
|  version  split       output pipeline ver     parent dataset ver  size    update time
| - v1      val         v1.run1                 v1[val]             25K     2023-02-01 19:00:00
Total 2 split, 3 version

Total 2 dataset
```

2. Compair specific dataset versions:
```shell
dataci dataset diff -n text_classification v3 v1

                v3   -----> v1
[train] <<< [P] train_data_pipeline <<< [D] pairwise_raw[train]
Size            200K        100K
Pipeline        v2.run2     v1.run1
Parent dataset  v2          v1
[val] <<< [P] val_data_pipeline <<< [D] pairwise_raw[val]
Pipeline        v1.run1     v1.run1
Parent dataset  v1          v1

Data-centric Benchmark Result
Desc            Data Aug. + Text Cls. + FT + S-BERT
ID              bench3      bench2
Train Metrics   AUC 0.88    AUC 0.82
                ACC 0.90    ACC 0.85
Val Metrics     AUC 0.75    AUC 0.70
                ACC 0.80    ACC 0.78

View detailed compare result at https://localhost:8888/dataset/text_classification/compare&to=v3&source=v1
```

## 4.2 What is the best performance?
```shell
dataci benchmark ls -desc=val/auc text_classification

Data Augmentation + Text Classification + Finetune + SentenceBERT
ID      dataset ver     train/auc   train/acc   val/auc     val/acc
bench3  v3              0.88        0.90        0.75        0.80              
bench2  v2              0.82        0.85        0.70        0.78
bench1  v1              0.75        0.86        0.70        0.72

Total 1 benchmark, 3 records
```

## 4.3 How many pipelines are built?
```shell
dataci pipeline ls -a
train_data_pipeline
text_classification[train] <<< train_data_pipeline <<< pairwise_raw[train]
version     stage   output dataset ver   input dataset ver  update time
v2          3       v3                   v2                 2023-02-02 19:00:00
                    v2                   v1                 2023-02-01 19:00:00
v1          3       v1                   v1                 2023-02-01 19:00:00

val_data_pipeline
text_classification[val] <<< train_data_pipeline <<< pairwise_raw[val]
version     stage   output dataset ver   input dataset ver  update time
v1          2       v1                   v1                 2023-02-01 19:00:00

Total 2 pipelines, 3 versions, 3 output dataset versions
```
