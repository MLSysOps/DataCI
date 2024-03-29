## 1. Initialize the project


## 3. Build CI for the workflow interactively

```shell
streamlit run config_ci.py
```

Add Action -> Create a new CI Action -> Add Jobs -> Manual Run
You can build the workflow interactively with the UI:

```shell
streamlit run config_ci.py
```

[![Add CI to Existing workflow]](https://github.com/MLSysOps/DataCI/assets/36268431/36e3bf70-b678-4fd9-945b-196afb829f79)

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
