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
import sys
import time
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import torch
import transformers
from sklearn.metrics import accuracy_score, roc_curve, auc
from torch.utils.collect_env import get_pretty_env_info
from torch.utils.data import Dataset, DataLoader
from transformers import AutoTokenizer, AutoModelForSequenceClassification

from dataci.decorators import stage

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
    parser.add_argument('--num_classes', type=int, default=2,
                        help='Number of classes in the dataset for classification.')
    parser.add_argument('--test_dataset', type=str, required=True,
                        help='Path to test dataset CSV file')
    parser.add_argument('--id_col', type=str, default='review_id',
                        help='Name of ID column in dataset')
    parser.add_argument('--text_col', type=str, default='text',
                        help='Name of text column in dataset')
    parser.add_argument('--label_col', type=str, default='stars',
                        help='Name of label column in dataset')
    parser.add_argument('-b', '--bs', '--batch_size', type=int, default=32, dest='batch_size',
                        help='Batch size for data loader')
    parser.add_argument('--model_name', type=str, default='bert-base-uncased',
                        help='Name of Hugging Face transformer model to use')
    parser.add_argument('--tokenizer_name', type=str, default='bert-base-uncased',
                        help='Name of Hugging Face tokenizer to use')
    parser.add_argument('--preprocessing_num_workers', type=int, default=4,
                        help='Num of workers used for preprocessing')
    parser.add_argument('--logging_steps', type=int, default=100,
                        help='Printing frequency during training')
    parser.add_argument('--exp_root', type=Path, default='output',
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


def setup_dataloader(args, tokenizer, logger):
    test_dataset = TextDataset(
        args.test_dataset, id_column=args.id_col, text_column=args.text_col, label_column=args.label_col,
        tokenizer=tokenizer,
    )
    logger.info(f'Test dataset size: {len(test_dataset)}')

    test_dataloader = DataLoader(
        test_dataset, batch_size=args.batch_size, collate_fn=collate_fn,
        num_workers=args.preprocessing_num_workers,
    )
    return test_dataloader


@torch.no_grad()
def val_one_epoch(args, model, dataloader, epoch_num, test=False, logger=...):
    stage_name = 'test' if test else 'val'
    val_loss_list, val_acc_list, batch_time_list = list(), list(), list()
    val_pred_results = list()
    model.eval()
    for idx, batch in enumerate(dataloader):
        batch_start_time = time.time()
        ids = batch.pop('ids')
        for k, v in batch.items():
            batch[k] = v.to(args.device)
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

        if idx % args.logging_steps == 0:
            logger.info(f'[Epoch{epoch_num}][Step {idx}] {stage_name}_loss={loss.item()}, {stage_name}_acc={acc}')

    val_loss_epoch = np.average(val_loss_list).item()
    val_acc_epoch = np.average(val_acc_list).item()
    val_batch_time_epoch = np.average(batch_time_list).item()
    logger.info(
        f"[Epoch: {epoch_num}] {stage_name}_loss_epoch={val_loss_epoch}, {stage_name}_acc_epoch={val_acc_epoch}, {stage_name}_batch_time_epoch={val_batch_time_epoch}"
    )

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


@stage(task_id='predict_text_classification')
def main(args=None):
    args = parse_args(args)

    # Prepare exp directory
    args.exp_root = args.exp_root / '_'.join([
        f'{datetime.now().strftime("%Y%m%d-%H%M%S")}',
        f"task=text_classification",
        # f"model={args.model_name}",
        f"b={args.batch_size}",
        f"j={args.preprocessing_num_workers}",
    ])
    args.exp_root.mkdir(exist_ok=True, parents=True)
    args.log_path = args.exp_root / LOG_FILE_NAME
    args.checkpoint_dir = args.exp_root / 'checkpoint'
    args.checkpoint_dir.mkdir(parents=True, exist_ok=True)
    args.metrics_dir = args.exp_root / 'metrics'
    args.metrics_dir.mkdir(parents=True, exist_ok=True)
    args.pred_dir = args.exp_root / 'pred'
    args.pred_dir.mkdir(parents=True, exist_ok=True)

    if args.seed:
        set_seed(args.seed)

    # Set up logging
    # 1. Make one log on every process with the configuration for debugging.
    transformers.utils.logging.set_verbosity_info()
    logging.basicConfig(
        format=f'%(asctime)s - %(levelname)s - %(name)s - %(message)s',
        datefmt="%m/%d/%Y %H:%M:%S",
        level=logging.INFO,
    )
    logger = logging.getLogger(__name__)
    # 2. Setup logging to file
    logger.addHandler(logging.FileHandler(args.log_path))

    # Collect environment information
    logger.info(get_pretty_env_info())

    # Load the tokenizer
    tokenizer = AutoTokenizer.from_pretrained(args.tokenizer_name)
    # Load dataset
    test_dataloader = setup_dataloader(args, tokenizer, logger=logger)
    # Load model
    model = AutoModelForSequenceClassification.from_pretrained(args.model_name, num_labels=args.num_classes)
    model = model.to(args.device)

    test_metrics_dict, test_pred_result = val_one_epoch(
        args, model, test_dataloader, epoch_num=None, test=True, logger=logger,
    )
    # Save test results and metrics
    logger.info(f"Saving test metrics and predictions to {args.exp_root}")
    # 1. Save test metrics to JSON file
    with open(args.metrics_dir / 'test_metrics.json', 'w') as f:
        json.dump(test_metrics_dict, f)
    # 2. Save test prediction to CSV file
    pd.DataFrame(test_pred_result).to_csv(args.pred_dir / 'test_preds.csv', index=False)

    return str(args.exp_root)


if __name__ == '__main__':
    main.test()
