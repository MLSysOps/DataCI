In this tutorial, we are going to build a text dataset for category classification. After this tutorial,
we will have a basic picture of how to use DataCI to manage different versions of datasets,
their data generating pipelines, and quickly adapt previous data scientists efforts to new versions of datasets.

This tutorial uses a simplified workflow from industry. Given a product title, we are going to determine the product
category.

# 0. Prerequisites

The scripts for this section is in `0.prerequisites.sh`, you can run in one click with:

```
bash 0.prerequisites.sh
```

## 0.1 Initialize DataCI

One-click initialization and start DataCI:

```shell
dataci standalone
```

We also provide a step-by-step initialization:

1. Init DataCI

```shell
dataci init
```

2. Connect Cloud Services

```shell
dataci connect s3 -k <your_s3_key> -p
# Your password will be prompted to input
```

3. Create a workspace `testspace`

```shell
dataci workspace use testspace
```

4. Start DataCI server

```shell
dataci start
```

## 1.2 Publish official stages and workflows

```shell
dataci stage publish action_hub/data_qc.py data_quality_check
dataci stage publish action_hub/benchmark/dc_bench.py data_centric_benchmark
dataci workflow publish action_hub/ci_cd_trigger.py trigger_ci_cd
```

# 1. Build Text Classification Dataset

Go to `example/create_text_classification_dataset` folder.

## 1.1 Check Sample Raw Data

Assume we have sampled 20K raw data from online product database, and hand over these raw data to annotators for
verify their product category which are filled by sellers and contains some noise. Now, the first batch of
10K finish labelling data are returned.

We can check the raw data at `s3://dataci-shared/text_cls_v1/train.csv`

This dataset contains train and val splits. Each split contains a CSV file with 3 columns:
`id`, `product_name` and `category_lv3`. We are going to build a pipeline to classify the product category
(`category_lv3`) based on its dirty `product_name`.

## 1.2 Publish raw data

The scripts for this section is in `1.1.publish_raw_data.sh`, you can run in one click with:

```
bash 1.2.publish_raw_data.sh
```

Publish the text dataset from the s3 data file URL.

Save the dataset to `testspace` workspace with name `text_raw_raw`:

```shell
dataci dataset save -n text_cls_raw s3://dataci-shared/text_cls_v1/train.csv
```

And publish the just saved dataset using its identifier:

```shell
dataci dataset publish text_cls_raw@1b
```

The dataset is automatically versioned as `text_cls_raw@1`.

## 1.3 Build a dataset for text classification

The scripts for this section is in `1.3.build_text_classification_dataset.py`, you can run in one click with:

```
python 1.3.build_text_classification_dataset.py
```

1. Build train dataset v1

```python
from dataci.decorators.stage import stage
from dataci.decorators.workflow import workflow


@stage()
def data_read(**context):
    from dataci.hooks.df_hook import DataFrameHook

    version = context['params'].get('version', None)
    df = DataFrameHook.read(dataset_identifier=f'text_cls_raw@{version}', **context)
    return df, 'product_name'


@stage()
def text_aug(inputs):
    import augly.text as txtaugs

    df, text_col = inputs

    transform = txtaugs.InsertPunctuationChars(
        granularity="all",
        cadence=5.0,
        vary_chars=True,
    )

    outputs = transform(df[text_col].tolist())
    df[text_col] = outputs
    return df


@stage()
def data_save(inputs, **context):
    from dataci.hooks.df_hook import DataFrameHook

    return DataFrameHook.save(name='text_aug_dataset', df=inputs, **context)


@workflow(
   name='build_text_dataset', 
   params={'version': 1},
)
def main():
    data_read >> text_aug >> data_save
```

Save the pipeline definition files at `example/create_text_classification_dataset/build_text_dataset.py`.

Test the train data workflow:

```python
# workflow 'main' from previous code
main = ...

import random

# Set seed for reproducibility
random.seed(42)

dataset_identifier = main()
print(dataset_identifier)
```

The output DataFrame will be automatically saved as a temp dataset `text_aug_dataset@d6` into the workspace.
We can further check the temp dataset.

2. Quality Check for the Dataset

```python
# dataset_identifier from previous code
dataset_identifier = ...

from dataci.models import Stage

data_qc = Stage.get('official.data_qc@latest')
try:
    data_qc(dataset_identifier=dataset_identifier)
    print('Data Quality Check Passed!')
except Exception as e:
    print('Data Quality Check Failed!', e)
```

3. Dataset benchmark

```python
# dataset_identifier from previous code
dataset_identifier = ...

from dataci.models import Stage

dc_benchmark = Stage.get('official.dc_bench@latest')
benchmark_result = dc_benchmark(dataset_identifier=dataset_identifier)
```

For demonstration purpose, we only train and validation the dataset for a few steps and obtain the results.

4. Publish data workflow

It looks great for the data workflow, we can publish it to the workspace for knowledge sharing.

```shell
# Make sure you are in the example/create_text_classification_dataset folder
dataci workflow publish build_text_dataset.py main
```

5. Publish first version of text dataset

```shell
dataci dataset publish text_aug_dataset@d6
```

# 2. Let's automate the data checking process as a CI/CD pipeline

The scripts for this section is in `2.try_with_new_data_augmentation_method.py`, you can run in one click with:

```
python 2.try_with_new_data_augmentation_method.py
```

From previous section, we have built a data workflow to create a text classification dataset. We have also applied
some basic checking and benchmarking on the dataset / data workflow before we publish them to the team workspace.
Since the dataset checking procedure is a common practice for the team, why not automate the process as a CI/CD?

And, with DataCI, we can easily build such a CI/CD workflow.

## 2.1 Define trigger of the CI/CD workflow

We can define the trigger of the CI/CD workflow when a new version of dataset is published to the workspace and
when a new version of some stages used in the workflow is published to the workspace.

```yaml
name: text_dataset_ci
on:
  dataset_publish: text_raw_train
  stage_publish: text_aug
```

## 2.2 Define workflow auto-search

Upon the trigger, we can automatically search for the combination of

1. all versions of the existing dataset
2. all versions of the care-about stage used in the workflow, which published from teams.
3. Fixed version of the workflow

```yaml
config:
  workflow: build_text_dataset@latest
  dataset: raw_dataset@*
  stage: text_aug@*
```

At the time of running, the runtime will build the workflow with searched dataset and stage version.

## 2.3 Define the CI/CD workflow

Let's define the CI/CD workflow with a given dataset and stage version. The used version of dataset, workflow and stage
are passed through the `config`. Therefore, we define a CI/CD workflow same as in Section 1, with the following steps:

1. Execute the built workflow `${{ config.workflow }}` with the given dataset version `${{ config.dataset.version }}`
2. Data Quality Check using the `official.data_qc` stage
3. Data-centric Benchmark using the `official.dc_bench` stage
4. Publish the workflow `${{ config.workflow }}` to the workspace
5. Publish the dataset from Step Execute Workflow output `${{ steps.Execute_Workflow.output }}` to the workspace

```yaml
jobs:
  steps:
    - name: Execute Workflow
      uses: ${{ config.workflow }}
      with:
        version: ${{ config.dataset.version }}
    - name: Data Quality Check
      uses: official/stages/data_qc@v1
    - name: Data-centric Benchmark
      uses: official/stages/dc_bench@v1
      with:
        type: data_aug
        ml_task: text_classification
        pass_condition: metrics.acc > 0.75
    - name: Publish Workflow
      run: dataci workflow publish ${{ config.workflow }}
    - name: Publish Dataset
      run: dataci dataset publish ${{ steps.Execute_Workflow.output }}
```

We combine 2.1 ~ 2.3 together and save the CI/CD workflow definition file at
`example/create_text_classification_dataset/ci.yaml`.

## 2.4 Publish the CI/CD workflow

We can build and publish the CI/CD workflow with:

```shell
dataci ci publish ci.yaml
```

We can see two workflows related to this CI/CD workflow are published to the workspace:

1. `text_dataset_ci_trigger` - trigger CI/CD, and auto-search combination of workflow / dataset versions to perform
   CI/CD jobs
2. `text_dataset_ci` - the CI/CD job workflow using the configured workflow / dataset versions

It takes about 1 minute for the CI/CD trigger in effect.  
Or you can restart DataCI server to make it effective immediately:

```shell
# Go to the terminal runs DataCI server, press Ctrl+C to stop the server
# Then run the following command to restart the server
# If the server is run at the background, you can use `ps aux | grep "dataci start"` to find the process id and kill it
dataci start
```

# 3.Now let's use knowledge from teammates to improve the dataset

Let's create a second version of `text_aug_dataset` for classification by applying a
different data augmentation method to improve the model performance.

## 3.1 Write a second version train data pipeline

We design another text augmentation method:

```python
from dataci.decorators.stage import stage

@stage()
def text_aug(inputs):
    import augly.text as txtaugs
    
    df, text_col = inputs

    transform = txtaugs.Compose(
        [
            txtaugs.InsertWhitespaceChars(p=0.5),
            txtaugs.InsertPunctuationChars(
                granularity="all",
                cadence=5.0,
                vary_chars=True,
            )
        ]
    )
    outputs = transform(df[text_col].tolist())
    df[text_col] = outputs
    return df
```

Note that you should save the `text_aug` v2 in a file, otherwise stage publish will not work.

## 3.2 Publish text augmentation method v2

```shell
dataci stage publish text_aug_stage_v2.py text_aug
```

Note that you should save the `text_aug` v2 in a file, otherwise stage publish will not work.

Upon the new data augmentation method `text_aug` is published to the workspace, the CI/CD workflow will
be automatically triggered. You can check the output at terminal running DataCI server.

# 4. Try with more data

Our human annotators have finished the 2nd batch 10K data labelling. We publish the v2 20K raw dataset directly from S3
file:

```shell
dataci dataset save -n text_cls_raw s3://dataci-shared/text_cls_v2/train.csv
dataci dataset publish text_cls_raw@2d
```

As we have defined the CI/CD workflow, the workflow will be triggered automatically upon the new dataset is published.
You can check the output at terminal running DataCI server.

# 5. Summary

## 4.1 How many datasets are built?

```shell
dataci dataset ls

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

## 4.2 Compair between different dataset versions

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

## 4.3 How many pipelines are built?

```shell
dataci workflow ls

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
