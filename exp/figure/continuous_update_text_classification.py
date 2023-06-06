#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: May 26, 2023
"""
from collections import OrderedDict

import matplotlib.pyplot as plt

from config import set_size, COLOR_LIST


def plot_online_service_quality_vs_time():
    service_quality_dict = [
        {'time': '2020 Dec', 'acc': 0.6786},
        {'time': '2021 Jan', 'acc': 0.6988},
        {'time': '2021 Jan', 'acc': 0.9446},
        {'time': '2021 Feb', 'acc': 0.9432},
        {'time': '2021 Mar', 'acc': 0.9415},
        {'time': '2021 Mar', 'acc': 0.9536},
        {'time': '2021 Apr', 'acc': 0.9518},
        {'time': '2021 Apr', 'acc': 0.9545},
    ]
    online_test_dict = {
        'v6': [
            {'time': '2020 Dec', 'acc': 0.6786},
            {'time': '2021 Jan', 'acc': 0.6988},
            {'time': '2021 Feb', 'acc': 0.6570},
            {'time': '2021 Mar', 'acc': 0.6487},
            {'time': '2021 Apr', 'acc': 0.6429},
        ],
        'v7': [
            {'time': '2021 Jan', 'acc': 0.9446},
            {'time': '2021 Feb', 'acc': 0.9432},
            {'time': '2021 Mar', 'acc': 0.9415},
            {'time': '2021 Apr', 'acc': 0.9415},
        ],
        'v8 (failed)': [
            {'time': '2021 Feb', 'acc': 0.9388},
            {'time': '2021 Mar', 'acc': 0.9373},
            {'time': '2021 Apr', 'acc': 0.9361},
        ],
        'v9': [
            {'time': '2021 Mar', 'acc': 0.9536},
            {'time': '2021 Apr', 'acc': 0.9518},
        ],
        'v10': [
            {'time': '2021 Apr', 'acc': 0.9545},
        ],
    }
    label_to_idx = OrderedDict()
    idx_counter = 0
    for data in service_quality_dict:
        if data['time'] not in label_to_idx:
            label_to_idx[data['time']] = idx_counter
            idx_counter += 1

    fig, (ax, ax2) = plt.subplots(2, 1, sharex=True, figsize=set_size())
    ax: 'plt.Axes'
    ax2: 'plt.Axes'
    for ax_ in (ax, ax2):
        # Plot service quality line chart
        idx = [label_to_idx[data['time']] for data in service_quality_dict]
        online_acc = [data['acc'] for data in service_quality_dict]
        ax_.plot(
            [*idx, idx[-1] + 1],
            [*online_acc, online_acc[-1]], alpha=0.2,
            label='Online Service', color='blue', linewidth=4, zorder=-1)
        # Plot online test line chart for each update
        for i, (update_name, update_data) in enumerate(online_test_dict.items()):
            idx = [label_to_idx[data['time']] for data in update_data]
            acc = [data['acc'] for data in update_data]
            ax_.plot(
                [*idx, idx[-1] + 1], [*acc, acc[-1]], '--', label=update_name, color=COLOR_LIST[i], marker='o', markersize=5, zorder=-1
            )

    # hide the spines between ax and ax2
    ax.spines['bottom'].set_visible(False)
    ax2.spines['top'].set_visible(False)

    ax2.set_xticks(list(set(label_to_idx.values())))
    ax2.set_xticklabels(label_to_idx.keys())
    ax.xaxis.set_visible(False)
    ax2.tick_params(top=False)  # don't put tick at the top

    ax.set_xlim(0.5, 4.5)
    ax.set_ylim(0.915, 0.975)
    ax2.set_xlim(0.5, 4.5)
    ax2.set_ylim(0.645, 0.705)
    ax.set_yticks([0.92, 0.94, 0.96])
    ax2.set_yticks([0.66, 0.68, 0.70])
    # Dummy subgraph label to get the correct spacing
    ax.set_ylabel('Accuracy', color='white')
    fig.text(0.04, 0.5, 'Accuracy', va='center', rotation='vertical')
    ax2.set_xlabel('Time')
    ax2.legend(bbox_to_anchor=(1, 0.1), loc='lower right')

    d = .015  # how big to make the diagonal lines in axes coordinates
    # arguments to pass to plot, just so we don't keep repeating them
    kwargs = dict(transform=ax.transAxes, color='gray', clip_on=False, linewidth=1)
    ax.plot((-d, +d), (-d, +d), **kwargs)  # top-left diagonal
    ax.plot((1 - d, 1 + d), (-d, +d), **kwargs)  # top-right diagonal

    kwargs.update(transform=ax2.transAxes)  # switch to the bottom axes
    ax2.plot((-d, +d), (1 - d, 1 + d), **kwargs)  # bottom-left diagonal
    ax2.plot((1 - d, 1 + d), (1 - d, 1 + d), **kwargs)  # bottom-right diagonal

    plt.tight_layout()
    plt.savefig('online_service_quality_vs_time.pdf')


if __name__ == '__main__':
    plot_online_service_quality_vs_time()
