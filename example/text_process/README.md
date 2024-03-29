In this tutorial, you can easily track every version of the input, output data and code for your existing
data pipeline with few lines of modification. You can also see the below example script [here :scroll:](./text_process_ci.py).

1. Write a data augmentation stage:

    ```python
    import augly.text as textaugs
    from dataci.plugins.decorators import stage
    
    
    @stage
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
   We write the workflow process in a function `text_process_ci` in a normal way, and then decorate it with `@dag` to
   convert it to a DataCI tracked workflow. With the `trigger` argument, we can specify the trigger event for the
   pipeline. In this example, the pipeline will be triggered when a new version of `yelp_review` dataset is published.

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
        # Publish the pipeline: text_process_ci@v1 and its stage text_augmentation@v1 will be tracked
        text_process_ci_pipeline.publish()
        # Run the pipeline on the server
        run_id = text_process_ci_pipeline.run()
    ```
   The text process CI pipeline will be triggered and run. Go
   to [pipeline runs dashboard](http://localhost:8080/taskinstance/list/?_flt_3_dag_id=testspace--text_process_ci--v1)
   to check the pipeline run result.  
   Note that the you should write pipeline testing and trigger code aside from the pipeline definition code.
   Otherwise, the pipeline will be triggered every time when you import the pipeline definition code.

4. Automatically trigger the CI pipeline by publish a new version of input dataset:

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
   Go to [pipeline runs dashboard](http://localhost:8080/taskinstance/list/?_flt_3_dag_id=default--text_process_ci--v1)
   to check the pipeline run result.