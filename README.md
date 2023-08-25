# Data CI

DataCI is a comprehensive platform for track your data-centric AI pipeline in dynamic streaming data settings. It 
provides rich APIs for seamless streaming dataset management, data-centric pipeline development, versioning 
and evaluation on streaming scenarios.

DataCI is featured for
- 🐣 **Easy to use**: enable DataCI management by only few lines of modification to your 
  existing data-centric functions and pipelines
- 🗂️ **Version Control**: Track versions of streaming data, data-centric pipelines, running results and their lineages
- 📊 **Auto Benchmark**: Evaluate pipeline automatically on new streaming data and data-centric functions

This project is still under development. Please feel free to open an issue if you have any questions or suggestions.
- [Installation](#installation-construction)
- [Quick Start](#quick-start-truck)
- [Examples (for Advanced Usages)](#examples-books)
- [Citation](#citation-seedling)
- [Contributors](#contributors-sparkles)
- [License](#lincese-page-facing-up)

## Installation :construction:

Manual install DataCI package from source code:
```shell
pip install -e .
```
:warning: For pipeline orchestration backend framework (i.e., [Apache-Airflow](https://airflow.apache.org/)), you may 
need to install manually since the version to meet your deployment can be different.
Here is a snapshot for [quick installation](https://airflow.apache.org/docs/apache-airflow/stable/installation/installing-from-pypi.html):
```shell
AIRFLOW_VERSION=2.6.1
PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
pip install "apache-airflow[celery]==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
```

Initialize DataCI by:

```shell
dataci init
```

:warning: This command will initialize the DataCI, you should run this command only once when installing.

Start DataCI server by:

```shell
dataci start
```

## Quick Start :truck:

With few lines of modification, you can easily track every version of the input, output data and code for your existing
data pipeline.

1. Write a data augmentation stage:

    ```python
    import augly.text as textaugs
    from dataci.plugins.decorators import stage as task
    
    
    @task
    def text_augmentation(df):
        aug_function = textaugs.ReplaceSimilarUnicodeChars()
        df['text'] = aug_function(df['text'].tolist())
        return df
    ```

2. Define the text processing CI pipeline:

    ```python
    from datetime import datetime
    from dataci.models import Event
    from dataci.plugins.decorators import dag, Dataset
    
    
    @dag(
        start_date=datetime(2020, 7, 30), schedule=None,
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
        Dataset(name='text_aug', dataset_files=text_aug_df)
    
    # Build the pipeline
    text_process_ci_pipeline = text_process_ci()
    ```

3. Manually run the pipeline:

    ```python
    from dataci.models import Dataset
    
    
    if __name__ == '__main__':
        # Prepare the input dataset, you can either provide a list or a file path
        yelp_review_dataset = Dataset('yelp_review', dataset_files=[
            {'date': '2020-10-05 00:44:08', 'review_id': 'HWRpzNHPqjA4pxN5863QUA', 'stars': 5.0, 'text': "I called Anytime on Friday afternoon about the number pad lock on my front door. After several questions, the gentleman asked me if I had changed the battery.",},
            {'date': '2020-10-15 04:34:49', 'review_id': '01plHaNGM92IT0LLcHjovQ', 'stars': 5.0, 'text': "Friend took me for lunch.  Ordered the Chicken Pecan Tart although it was like a piece quiche, was absolutely delicious!",},
            {'date': '2020-10-17 06:58:09', 'review_id': '7CDDSuzoxTr4H5N4lOi9zw', 'stars': 4.0, 'text': "I love coming here for my fruit and vegetables. It is always fresh and a great variety. The bags of already diced veggies are a huge time saver.",},
        ]).publish(version_tag='2020-10')
        # Test the pipeline locally
        text_process_ci_pipeline.test()
        # Publish the pipeline
        text_process_ci_pipeline.publish()
        # Run the pipeline on the server
        run_id = text_process_ci_pipeline.run()
    ```
    Note that the you should write pipeline testing and trigger code aside from the pipeline definition code.
    Otherwise, the pipeline will be triggered every time when you import the pipeline definition code.
    
    The text process CI pipeline will be triggered and run. Go to [pipeline runs dashboard](http://localhost:8080/taskinstance/list/?_flt_3_dag_id=testspace--text_process_ci--v1)
    to check the pipeline run result.

4. Automatically trigger the pipeline by publish a new version of input dataset:

    ```python
    from dataci.models import Dataset
    
    
    if __name__ == '__main__':
        # DataCI auto track the new version of yelp_review dataset
        yelp_review_dataset = Dataset('yelp_review', dataset_files=[
            {'date': '2020-11-02 00:59:29', 'review_id': 'LTr95e6eOmLc7S_1WxM88Q', 'stars': 5.0, 'text': "First of all the owner and staff went above and beyond to make us feel comfortable and protected during covid.  Secondly, the bar had fantastic drinks",},
            {'date': '2020-11-16 22:07:48', 'review_id': 'UhA9H0LK59qZegWOxyotcA', 'stars': 5.0, 'text': "Herbies is awesome! My brunch was perfect, service, food, and drinks! I will definitely continue coming back.",},
            {'date': '2020-11-18 06:38:46', 'review_id': 'YqTMxlbebNBDcKYTIUvrdw', 'stars': 5.0, 'text': "You won't regret stopping here, hidden gem with great food and a laid back and comfortable atmosphere, a place you can gather with friends and they will treat you like family while you're there.",},
        ]).publish(version_tag='2020-11')
    ```
    Before executing the above code, please wait some time for the previous manual triggered run to finish.
    Upon the publish of the new version of input dataset, the text process CI pipeline is automatically 
    triggered and evaluate on the new version of streaming data. 
    Go to [pipeline runs dashboard](http://localhost:8080/taskinstance/list/?_flt_3_dag_id=testspace--text_process_ci--v1)
    to check the pipeline run result.

You may see the above example script [here](./example/text_process_ci.py).

## Examples :books:

We have provided several examples to demonstrate the advanced usage of DataCI:

- [Play with the CI](./example/ci/README.md)
- [Build Text Classification Dataset](./example/create_text_classification_dataset) [Jupyter Notebooks](./example/create_text_classification_dataset/Create_Text_Classification_Dataset.ipynb)

## Citation :seedling:

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

## Contributors :sparkles:
- Yuanming Li
- Huaizheng Zhang
- Yizheng Huang

## License :page_facing_up:
This repository is open-sourced under [MIT License](./LICENSE).
