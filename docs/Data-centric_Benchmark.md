Great! We have just create a `text_classification` dataset with train and test splits.
Now, we can easily train any SOTA text model and get its performance.

We have provide a easy-to-use data-centric benchmark tool. We can benchmark the dataset by training a sentence BERT
model.
```python
from dataci.dataset import get_dataset
from dataci.benchmark import Benchmark


train_dataset = get_dataset('text_classification@latest')
test_dataset = get_dataset('text_raw_val@latest')

Benchmark(
    type='data_augmentation',
    ml_task='text_classification',
    model='bert-base-cased',
    train_dataset=train_dataset,
    test_dataset=test_dataset,
)
# store metrics with the train, test dataset
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