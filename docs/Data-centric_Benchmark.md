Great! We have just create a `text_classification` dataset with train and test splits.
Now, we can easily train any SOTA text model and get its performance.

We have provide a easy-to-use data-centric benchmark tool. We can benchmark the dataset by training a sentence BERT
model.
```python
metrics = run_benchmark(
    type='data_augmentation',
    ml_task='text_classification',
    model='sentencebert',
    dataset={'name': 'text_classification', 'version': 'latest', 'feature_col': 'to_product_name', 'label_col': 'cateogry_lv1'},
    splits=['train', 'val']
)
# store metrics with the train, test dataset
```