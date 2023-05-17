# Data CI

A tool to record data-science triplets (🗂️ `dataset`, 📏 `pipeline`, 📊 `results`)

- 🗂️ Manage data with accumulated human data-pipeline-building knowledge
- 📏 Easy pipeline debugging with interatives
- 📊 One-click data-centric benchmarking with comprehensive dataset report

## Installation

```shell
pip install -e .
```

### PyPI install

### Manual install

- `s3fs-fuse` is required for S3 support. Please follow the [installation guide](https://github.com/s3fs-fuse/s3fs-fuse)
  to install it.

## Quick Start

1. Initialize the project

```shell
cd example/ci
bash init.sh
```

2. Build Text Augmentation Workflow V1

```shell
python build_workflow_v1.py
```

3. Build and add CI to existing workflow interactively

```shell
streamlit run config_ci.py
```

[![Add CI to Existing workflow](https://clipchamp.com/watch/6LETyy5UnZO)]

4. A new version of input dataset or augmentation method will trigger the CI automatically

## More Examples

- [Play with the CI](./example/ci/README.md)
- [Build Text Classification Dataset](./docs/Create_Text_Classification_Dataset.md) [Jupyter Notebooks](./docs/Create_Text_Classification_Dataset.ipynb)
- [Data-centric Benchmark: Compare Different Dataset Versions](./docs/Data-centric_Benchmark.md)
- [Data Analysis for Performance Checking and Debugging](./docs/Data_Analysis.md)

## Contributors

## License
