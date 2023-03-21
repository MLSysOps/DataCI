import subprocess

import augly.text as txtaugs

from dataci.pipeline import Pipeline, stage

print('Build train dataset v1')


# Data processing: text augmentation
@stage(inputs='pairwise_raw_train', outputs='text_aug.csv')
def text_augmentation(inputs):
    transform = txtaugs.InsertPunctuationChars(
        granularity="all",
        cadence=5.0,
        vary_chars=True,
    )
    inputs['product_name'] = inputs['product_name'].map(transform)
    return inputs


# Define data pipeline
train_data_pipeline = Pipeline(name='train_data_pipeline', stages=[text_augmentation])
train_data_pipeline.build()

print('Debug/test run the train data pipeline:')
train_data_pipe_run = train_data_pipeline()

# Run benchmarking
print('Benchmark the performance of the built classification dataset -->')
print('Train bert-base-uncased using the text classification dataset:')
cmd = [
    'python', 'train.py', '--train_dataset=train_data_pipeline/latest/runs/1/feat/text_aug.csv',
    '--test_dataset=../../data/text_raw/val.csv', '-b4', '--max_steps_per_epoch=20',
]
print('Executing command:', ' '.join(cmd))
subprocess.call(cmd)

# Publish
print('Publish train data pipeline')
train_data_pipeline.publish()
print('Run train_data_pipeline and publish the output dataset (i.e., train_data_pipeline:text_aug.csv):')
train_data_pipeline()
