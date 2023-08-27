#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Mar 22, 2023
"""
import argparse
import json
import logging
import random
import shutil
import sys
import tempfile
import time
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import torch
import transformers
from sklearn.metrics import accuracy_score, auc, roc_curve
from torch.optim import AdamW
from torch.utils.collect_env import get_pretty_env_info
from torch.utils.data import Dataset, DataLoader, random_split
from transformers import AutoTokenizer, AutoModelForSequenceClassification

from dataci.plugins.decorators import stage

LOG_FILE_NAME = 'out.log'


class TextDataset(Dataset):
    def __init__(self, csv_file, id_column, text_column, label_column, tokenizer):
        self.data = pd.read_csv(csv_file, dtype={id_column: str})
        self.id_column = id_column
        self.text_column = text_column
        self.label_column = label_column
        self.tokenizer = tokenizer
        print(self.data.head(5))

    def __len__(self):
        return len(self.data)

    def __getitem__(self, idx):
        text = self.data.iloc[idx][self.text_column]
        # stars > 3 positive, stars <= 3 negative
        label_id = int(self.data.iloc[idx][self.label_column] > 3)
        label = torch.tensor(label_id).long()
        tokens = self.tokenizer(text, padding=False, truncation=True, max_length=256, return_tensors='pt')
        return {
            'ids': self.data.iloc[idx][self.id_column],
            'input_ids': tokens['input_ids'].squeeze(), 'attention_mask': tokens['attention_mask'].squeeze(),
            'labels': label,
        }


def parse_args(args=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--split_ratio', type=float, default=0.95,
                        help='Ratio of training data to total data')
    parser.add_argument('--num_classes', type=int, default=2,
                        help='Number of classes in the dataset for classification.')
    parser.add_argument('--train_dataset', type=str, required=True,
                        default='processed/data_aug_2021Q1_aug1.csv',
                        help='Path to training dataset CSV file')
    parser.add_argument('--test_dataset', type=str, required=True,
                        default='data/reviews_2020Q4_val.csv',
                        help='Path to test dataset CSV file')
    parser.add_argument('--id_col', type=str, default='review_id',
                        help='Name of ID column in dataset')
    parser.add_argument('--text_col', type=str, default='text',
                        help='Name of text column in dataset')
    parser.add_argument('--label_col', type=str, default='stars',
                        help='Name of label column in dataset')
    parser.add_argument('--max_train_steps_per_epoch', type=int, default=1e38,
                        help='Max number of train steps for each epoch. For debug/demo purpose.')
    parser.add_argument('--max_val_steps_per_epoch', type=int, default=1e38,
                        help='Max number of val and test steps for each epoch. For debug/demo purpose.')
    parser.add_argument('-b', '--bs', '--batch_size', type=int, default=512, dest='batch_size',
                        help='Batch size for data loader')
    parser.add_argument('-e', '--epochs', type=int, default=3,
                        help='Number of epochs for training')
    parser.add_argument('--model_name', type=str, default='bert-base-uncased',
                        help='Name of Hugging Face transformer model to use')
    parser.add_argument('--tokenizer_name', type=str, default='bert-base-uncased')
    parser.add_argument('--learning_rate', type=float, default=1e-5,
                        help='Learning rate for optimizer')
    parser.add_argument('--preprocessing_num_workers', type=int, default=4,
                        help='Num of workers used for preprocessing')
    parser.add_argument('--logging_steps', type=int, default=100,
                        help='Printing frequency during training')
    parser.add_argument('--exp_root', type=str, default='output',
                        help='Path to save experiment results')
    parser.add_argument('--seed', type=int, default=42,
                        help='Random seed for reproducibility')

    # Add device argument
    parser.add_argument('--device', type=str, default='cuda' if torch.cuda.is_available() else 'cpu',
                        help='Device to use for training (cuda or cpu)')

    # allow providing args by sys.argv or args argument
    if args is not None:
        args = sys.argv[1:] + list(args)
    else:
        args = sys.argv[1:]

    args = parser.parse_args(args)
    return args


def set_seed(seed):
    random.seed(seed)
    np.random.seed(seed)
    torch.manual_seed(seed)
    if torch.cuda.is_available():
        torch.cuda.manual_seed_all(seed)


def collate_fn(batch):
    max_len = max([len(item['input_ids']) for item in batch])
    new_batch = dict()
    for data in batch:
        padding_length = max_len - len(data['input_ids'])
        data['input_ids'] = torch.cat([data['input_ids'], torch.zeros(padding_length, dtype=torch.int)], dim=-1)
        data['attention_mask'] = torch.cat([data['attention_mask'], torch.zeros(padding_length, dtype=torch.int)],
                                           dim=-1)
    example = batch[0]
    for k in example.keys():
        # Pack as a PyTorch tensor
        if isinstance(example[k], torch.Tensor):
            new_batch[k] = torch.stack([item[k] for item in batch])
        else:
            # Pack as a list
            new_batch[k] = [item[k] for item in batch]

    return new_batch


def setup_dataloader(
        train_dataset, test_dataset, split_ratio, tokenizer, batch_size,
        preprocessing_num_workers, id_col, text_col, label_col, logger
):
    train_dataset = TextDataset(
        train_dataset, id_column=id_col, text_column=text_col, label_column=label_col,
        tokenizer=tokenizer,
    )
    test_dataset = TextDataset(
        test_dataset, id_column=id_col, text_column=text_col, label_column=label_col,
        tokenizer=tokenizer,
    )

    # Split the dataset into training and validation sets
    train_size = int(split_ratio * len(train_dataset))
    val_size = len(train_dataset) - train_size
    train_dataset, val_dataset = random_split(train_dataset, [train_size, val_size])

    logger.info(f'Train split size: {train_size}, Val split size: {val_size}')
    logger.info(f'Test dataset size: {len(test_dataset)}')

    train_dataloader = DataLoader(
        train_dataset, batch_size=batch_size, collate_fn=collate_fn,
        num_workers=preprocessing_num_workers,
    )
    val_dataloader = DataLoader(
        val_dataset, batch_size=batch_size, collate_fn=collate_fn,
        num_workers=preprocessing_num_workers,
    )
    test_dataloader = DataLoader(
        test_dataset, batch_size=batch_size, collate_fn=collate_fn,
        num_workers=preprocessing_num_workers,
    )
    return train_dataloader, val_dataloader, test_dataloader


def train_one_epoch(
        model, dataloader, optimizer, epoch_num, max_train_steps_per_epoch, device, logger, logging_steps
):
    train_loss_list, train_acc_list, batch_time_list = list(), list(), list()
    train_pred_results = list()
    model.train()
    print('Length of dataloader: ', len(dataloader))
    for idx, batch in enumerate(dataloader):
        batch_start_time = time.time()
        optimizer.zero_grad()
        ids = batch.pop('ids')
        for k, v in batch.items():
            batch[k] = v.to(device)
        labels = batch['labels']
        outputs = model(**batch)
        loss = outputs.loss
        train_loss_list.append(loss.item())
        loss.backward()
        optimizer.step()
        batch_end_time = time.time()
        batch_time_list.append(batch_end_time - batch_start_time)

        # Calculate train accuracy
        logits = outputs.logits
        probabilities = torch.softmax(logits, dim=-1).detach().cpu().numpy()
        predictions = torch.argmax(logits, dim=-1).cpu().numpy()
        labels = labels.cpu().numpy()
        acc = accuracy_score(labels, predictions)
        train_acc_list.append(acc)

        # Post-process labels and predictions
        # 1. label id to label name
        # 2. probabilities to list of label name -> probability

        for id_, label, pred, probs in zip(ids, labels, predictions, probabilities):
            train_pred_results.append({
                'id': id_,
                'label': label,
                'prediction': pred,
                'probabilities': probs.tolist(),
            })

        if idx > max_train_steps_per_epoch:
            # Reach the max steps for this epoch, skip to next epoch
            break
        if idx % logging_steps == 0:
            logger.info(f'[Epoch{epoch_num}][Step {idx}] train_loss={loss.item()}, train_acc={acc}')

    train_loss_epoch = np.average(train_loss_list).item()
    train_acc_epoch = np.average(train_acc_list).item()
    batch_time_epoch = np.average(batch_time_list).item()
    logger.info(f"[Epoch {epoch_num}] train_loss_epoch={train_loss_epoch}, train_acc_epoch={train_acc_epoch}")

    train_pred_df = pd.DataFrame(train_pred_results)
    pred = train_pred_df['probabilities'].apply(lambda x: x[1])
    fpr, tpr, _ = roc_curve(train_pred_df['label'], pred)
    train_auc = auc(fpr, tpr)

    train_metrics_dict = {
        'loss': train_loss_epoch,
        'acc': train_acc_epoch,
        'auc': train_auc,
        'batch_time': batch_time_epoch,
        'losses': train_loss_list,
        'accs': train_acc_list,
        'batch_times': batch_time_list,
    }

    return train_metrics_dict, train_pred_results


@torch.no_grad()
def val_one_epoch(
        model, dataloader, epoch_num, max_val_steps_per_epoch, logger, logging_steps, device, test=False
):
    stage_name = 'test' if test else 'val'
    val_loss_list, val_acc_list, batch_time_list = list(), list(), list()
    val_pred_results = list()
    model.eval()
    for idx, batch in enumerate(dataloader):
        batch_start_time = time.time()
        ids = batch.pop('ids')
        for k, v in batch.items():
            batch[k] = v.to(device)
        labels = batch['labels']
        outputs = model(**batch)
        loss = outputs.loss
        val_loss_list.append(loss.item())
        batch_end_time = time.time()

        # Calculate train accuracy
        logits = outputs.logits
        probabilities = torch.softmax(logits, dim=-1).detach().cpu().numpy()
        predictions = torch.argmax(logits, dim=-1).cpu().numpy()
        labels = labels.cpu().numpy()
        acc = accuracy_score(labels, predictions)
        val_acc_list.append(acc)
        batch_time_list.append(batch_end_time - batch_start_time)

        # Post-process labels and predictions
        # 1. label id to label name
        # 2. probabilities to list of label name -> probability
        for id_, label, pred, probs in zip(ids, labels, predictions, probabilities):
            val_pred_results.append({
                'id': id_,
                'label': label,
                'prediction': pred,
                'probabilities': probs.tolist(),
            })

        if idx > max_val_steps_per_epoch:
            # Reach the max steps for this epoch, skip to next epoch
            break

        if idx % logging_steps == 0:
            logger.info(f'[Epoch{epoch_num}][Step {idx}] {stage_name}_loss={loss.item()}, {stage_name}_acc={acc}')

    val_loss_epoch = np.average(val_loss_list).item()
    val_acc_epoch = np.average(val_acc_list).item()
    val_batch_time_epoch = np.average(batch_time_list).item()
    logger.info(
        f"[Epoch: {epoch_num}] {stage_name}_loss_epoch={val_loss_epoch}, {stage_name}_acc_epoch={val_acc_epoch}, {stage_name}_batch_time_epoch={val_batch_time_epoch}"
    )

    # Calculate AUC
    val_pred_df = pd.DataFrame(val_pred_results)
    pred = val_pred_df['probabilities'].apply(lambda x: x[1])
    fpr, tpr, _ = roc_curve(val_pred_df['label'], pred)
    val_auc = auc(fpr, tpr)

    val_metrics_dict = {
        f'loss': val_loss_epoch,
        f'acc': val_acc_epoch,
        f'auc': val_auc,
        f'batch_time': val_batch_time_epoch,
        f'losses': val_loss_list,
        f'accs': val_acc_list,
        f'batch_times': batch_time_list,
    }

    return val_metrics_dict, val_pred_results


@stage(task_id='train_text_classification_model', multiple_outputs=True)
def main(
        *,
        train_dataset: str, test_dataset: str, exp_root: str = 'outputs',
        num_classes: int = 2, split_ratio: float = 0.95,
        tokenizer_name: str = 'bert-base-uncased',
        id_col: str = 'review_id', text_col: str = 'text', label_col: str = 'stars',
        model_name: str = 'bert-base-uncased', learning_rate: float = 1e-5, batch_size: int = 512,
        preprocessing_num_workers: int = 4,
        epochs: int = 3, max_train_steps_per_epoch: int = 1e38, max_val_steps_per_epoch: int = 1e38,
        logging_steps: int = 100,
        seed: int = 42, device: str = 'cuda' if torch.cuda.is_available() else 'cpu', **kwargs
):
    # Prepare exp directory
    exp_root = Path(exp_root)
    exp_root = exp_root / '_'.join([
        f'{datetime.now().strftime("%Y%m%d-%H%M%S")}',
        f"task=text_classification",
        # f"model={model_name}",
        f"lr={learning_rate:.2E}",
        f"b={batch_size}",
        f"j={preprocessing_num_workers}",
    ])
    print(f"Experiment root directory: {exp_root}")
    exp_root.mkdir(exist_ok=True, parents=True)
    log_path = exp_root / LOG_FILE_NAME
    checkpoint_dir = exp_root / 'checkpoint'
    checkpoint_dir.mkdir(parents=True, exist_ok=True)
    metrics_dir = exp_root / 'metrics'
    metrics_dir.mkdir(parents=True, exist_ok=True)
    pred_dir = exp_root / 'pred'
    pred_dir.mkdir(parents=True, exist_ok=True)

    if seed:
        set_seed(seed)

    # Set up logging
    # 1. Make one log on every process with the configuration for debugging.
    transformers.utils.logging.set_verbosity_info()
    logging.basicConfig(
        format=f'%(asctime)s - %(levelname)s - %(name)s - %(message)s',
        datefmt="%m/%d/%Y %H:%M:%S",
        level=logging.INFO,
    )
    logger = logging.getLogger(__name__)
    # 2. Setup logging to file. We log to a temp dir first, and copy to log dir per epoch
    tmp_log_path = tempfile.NamedTemporaryFile(mode='w')
    logger.addHandler(logging.FileHandler(tmp_log_path.name))

    # Collect environment information
    logger.info(get_pretty_env_info())

    # Load the tokenizer
    tokenizer = AutoTokenizer.from_pretrained(tokenizer_name)
    # Load dataset
    train_dataloader, val_dataloader, test_dataloader = setup_dataloader(
        train_dataset=train_dataset, test_dataset=test_dataset, batch_size=batch_size,
        preprocessing_num_workers=preprocessing_num_workers, split_ratio=split_ratio,
        id_col=id_col, text_col=text_col, label_col=label_col,
        tokenizer=tokenizer, logger=logger
    )
    # Load model
    model = AutoModelForSequenceClassification.from_pretrained(model_name, num_labels=num_classes)
    model = model.to(device)

    # Set up the optimizer
    optimizer = AdamW(model.parameters(), lr=5e-5)

    # Train the model
    for epoch in range(epochs):
        # train loop
        train_metrics_dict, train_pred_result = train_one_epoch(
            model=model, optimizer=optimizer, dataloader=train_dataloader, epoch_num=epoch,
            max_train_steps_per_epoch=max_train_steps_per_epoch,
            device=device, logger=logger, logging_steps=logging_steps,
        )
        # Validation loop
        val_metrics_dict, val_pred_result = val_one_epoch(
            model=model, dataloader=test_dataloader, epoch_num=epoch, max_val_steps_per_epoch=max_val_steps_per_epoch,
            test=True, device=device, logger=logger, logging_steps=logging_steps,
        )

        # Save loggings and results
        logger.info(f"Saving model checkpoint, metrics, and predictions to {exp_root}")
        # 1. Save model checkpoint
        checkpoint_dict = {
            'epoch': epoch,
            'name': model_name,
            'state_dict': model.state_dict(),
            'optimizer': optimizer.state_dict(),
        }
        torch.save(checkpoint_dict, checkpoint_dir / f"epoch={epoch}.pt")
        # 2. Save metrics to JSON file
        with open(metrics_dir / f'train_metrics_epoch={epoch}.json', 'w') as f:
            json.dump(train_metrics_dict, f)
        with open(metrics_dir / f'val_metrics_epoch={epoch}.json', 'w') as f:
            json.dump(val_metrics_dict, f)
        # 3. Save prediction to CSV file
        pd.DataFrame(train_pred_result).to_csv(pred_dir / f'train_preds_epoch={epoch}.csv', index=False)
        pd.DataFrame(val_pred_result).to_csv(pred_dir / f'val_preds_epoch={epoch}.csv', index=False)
        # 4. Copy log file
        shutil.copy(tmp_log_path.name, log_path)

    test_metrics_dict, test_pred_result = val_one_epoch(
        model=model, dataloader=test_dataloader, epoch_num=0, max_val_steps_per_epoch=max_val_steps_per_epoch,
        test=True, device=device, logger=logger, logging_steps=logging_steps,
    )
    # Save final model
    model.save_pretrained(exp_root / 'model')
    # Save test results and metrics
    logger.info(f"Saving test metrics and predictions to {exp_root}")
    # 1. Save test metrics to JSON file
    with open(metrics_dir / 'test_metrics.json', 'w') as f:
        json.dump(test_metrics_dict, f)
    # 2. Save test prediction to CSV file
    pd.DataFrame(test_pred_result).to_csv(pred_dir / 'test_preds.csv', index=False)
    # 3. Copy log file
    shutil.copy(tmp_log_path.name, log_path)

    return {
        'output': str(exp_root), 'model': str(exp_root / 'model')
    }


if __name__ == '__main__':
    args = parse_args()
    main.test(**vars(args))
