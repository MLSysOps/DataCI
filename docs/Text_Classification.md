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
cp -r dataset/multimodal_pairwise_v1 data/pairwise_raw/
# |- train
# |   |- 202211_pairwise.csv
# |- val
#     |- 202211_pairwise.csv
```

# 1. Build Text Classification Dataset

## 1.1 Publish raw data

Add this dataset with two split into the data repository.
```shell
python dataci/command/dataset.py publish -n pairwise_raw data/pairwise_raw
```

## 1.2 Build a dataset for text classification 

1. Build train dataset v1
```python
import unicodedata
import textclean
import augly.text as txtaugs


@stage(inputs='dataset:pairwise_raw[train]', dependency='auto', outputs='feat:text_clean')
def text_clean_train(data):
    text = data['text']
    # remove emoji, space, and to lower case
    text = clean(text, to_ascii=False, lower=True, normalize_whitespace=True, no_emoji=True)
    
    # remove accent
    data['text'] = ''.join(c for c in unicodedata.normalize('NFD', text) if unicodedata.category(c) != 'Mn')

    return data


@stage(inputs='feat:text_clean', dependency='auto', outputs='feat:text_aug')
def text_augementation(data):
    data['text'] = txtaugs.insert_punctuation_chars(
        data['text'],
        granularity="all",
        cadence=5.0,
        vary_chars=True,
    )
    return data


train_data_pipeline = Pipeline([text_clean, text_augmentation])
```

Run the train data pipeline:
```python
train_data_pipeline()
```
The output `text_aug.feat` will be used as train dataset.

2. Build validation dataset v1
```python
@stage(inputs='dataset:pairwise_raw[val]', dependency='auto', outputs='feat:text_clean')
def text_clean_val(data):
    # remove emoji, space, and to lower case
    text = clean(text, to_ascii=False, lower=True, normalize_whitespace=True, no_emoji=True)
    
    # remove accent
    text = ''.join(c for c in unicodedata.normalize('NFD', text) if unicodedata.category(c) != 'Mn')

    return text


val_data_pipeline = Pipeline([text_clean_val])
```

Run the validation data pipeline:
```python
val_data_pipeline()
```
The output `text_clean.feat` will be used as validation dataset. 

3. Run training with the built train and val dataset v1
Now you can apply a simple text classification training on a pre-trained BERT:
```shell
python train.py --train-dataset ./train_data_pipeline:text_aug --val-dataset ./val_data_pipeline:text_clean
```

4. Save data pipeline

You can now publish your data pipeline for a better management.
```python
train_data_pipeline.publish()
val_data_pipeline.publish()
# mkdir -p train_data_pipeline
# cd train_data_pipeline
# dvc stage add -n text_clean \
#     -d text_clean.py -d ../data/pairwise_raw/train -o text_clean_0.csv \
#     python text_clean.py
# dvc stage add -n text_augmentation \
#     -d text_augmentation.py -d text_clean_0.csv -o text_augmentation_0.csv \
#     python text_augmentation.py
```

## 1.3 Publish first version of text dataset

```shell
python dataci/dataset.py publish -n text_classification \
  -s train=train_data_pipeline:text_aug,val=val_data_pipeline:text_clean
```
Publish a new dataset `text_classification` using previous cached feature.  
The new dataset train split uses the latest feature `text_aug` output by pipeline `train_data_pipeline`.  
The new dataset val split uses the latest feature `text_aug` output by pipeline `val_data_pipeline`.

# 2. Try with New Data Augmentation Method

Let's create a second version of `text_dataset` with different data augmentation method to improve the model performance.

## 2.1 Write a second version train data pipeline
We design a better data augmentation method for `train_data_pipeline_v2`:
```python
@stage(inputs='feat:text_clean', dependency='auto', outputs='feat:text_aug')
def better_text_augementation(data):
    text = txtaugs.insert_punctuation_chars(
        data['text']
        granularity="all",
        cadence=5.0,
        vary_chars=True,
    )
    text = textaugs.insert_whitespace_chars(text)
    text = textaugs.insert_zero_width_chars(text)
    data['text'] = text
    return data


train_data_pipeline_v2 = Pipeline([text_clean, better_text_augementation])
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
train_data_pipeline(dataset_update=True)
# [D] pairwise_raw@v1[train] >>> train_data_pipeline@v2.run1 >>> [D] text_classification@v2[train]
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
