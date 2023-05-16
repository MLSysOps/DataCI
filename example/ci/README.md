## 1. Initialize the project

```shell
bash init.sh
```

## 2. Build Text Augmentation Workflow V1

```shell
python build_workflow_v1.py
```

## 3. Build CI for the workflow interactively

```shell
streamlit run config_ci.py
```

Add Action -> Create a new CI Action -> Add Jobs -> Manual Run

## 4. Use a new version of input dataset

```shell
dataci dataset save -n text_cls_raw s3://dataci-shared/text_cls_v2/train.csv
dataci dataset publish text_cls_raw@2d
```

And configure the workflow to use the new dataset version using the UI:
Input Data -> Select the input dataset -> Select the new version -> Use this version

You can see the configured CI is automatically triggered.

## 5. Use a new version of the text augmentation method

```shell
python text_aug_stage_v2.py
```

And configure the workflow to use the new stage version using the UI:
Edit -> Select the stage -> Select the new version -> Use this version

You can see the configured CI is automatically triggered.
