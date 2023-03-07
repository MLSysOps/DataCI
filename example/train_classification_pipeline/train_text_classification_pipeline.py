import os

import augly.text as txtaugs
import pandas as pd
import unicodedata
from cleantext import clean

from dataci.pipeline import Pipeline, stage


def clean_func(text):
    # remove emoji, space, and to lower case
    text = clean(text, to_ascii=False, lower=True, normalize_whitespace=True, no_emoji=True)

    # remove accent
    text = ''.join(c for c in unicodedata.normalize('NFD', text) if unicodedata.category(c) != 'Mn')

    return text


@stage(name='text_clean', inputs='pairwise_raw[train]', outputs='text_clean.csv')
def text_clean(inputs):
    df = pd.read_csv(os.path.join(inputs, 'train.csv'))
    df['to_product_name'] = df['to_product_name'].map(clean_func)
    return df


@stage(name='text_augmentation', inputs='text_clean.csv', outputs='text_augmentation.csv')
def text_augmentation(inputs):
    df = pd.read_csv(inputs)
    transform = txtaugs.InsertPunctuationChars(
        granularity="all",
        cadence=5.0,
        vary_chars=True,
    )
    df['to_product_name'] = df['to_product_name'].map(transform)
    return df

# Debug run
print('Debug run')
print('Running text clean')
text_clean()
print('Running text augmentation')
text_augmentation()
# Define a pipeline
print('Define a pipeline')
pipeline = Pipeline('train_text_classification')
pipeline.add_stage(text_clean)
pipeline.add_stage(text_augmentation)
pipeline.build()
print('Run pipeline')
pipeline()
print('Publish pipeline')
pipeline.publish()
