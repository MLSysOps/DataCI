#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: May 26, 2023
"""
import matplotlib.pyplot as plt

from config import set_size


def plot_online_service_quality_vs_time():
    service_quality_dict = [
        {'time': '2021 Jan', 'online_acc': 0.6988, 'a_b_test_acc': 0.9446, 'is_deploy': True},
        {'time': '2021 Feb', 'online_acc': 0.9432, 'a_b_test_acc': 0.9388, 'is_deploy': False},
        {'time': '2021 Mar', 'online_acc': 0.9373, 'a_b_test_acc': 0.9536, 'is_deploy': True},
        {'time': '2021 Apr', 'online_acc': 0.9518, 'a_b_test_acc': 0.9545, 'is_deploy': True}
    ]
    idx, labels, online_acc = [], [], []
    previous_idx, previous_acc = [], []
    deploy_idx, deploy_acc = [], []
    not_deploy_idx, not_deploy_acc = [], []
    for i, data in enumerate(service_quality_dict):
        labels.append(data['time'])
        # For online accuracy for previous service
        idx.append(i)
        online_acc.append(data['online_acc'])
        previous_idx.append(i)
        previous_acc.append(data['online_acc'])
        # For new service
        if data['is_deploy']:
            idx.append(i)
            online_acc.append(data['a_b_test_acc'])
            deploy_idx.append(i)
            deploy_acc.append(data['a_b_test_acc'])
        else:
            not_deploy_idx.append(i)
            not_deploy_acc.append(data['a_b_test_acc'])

    fig, (ax, ax2) = plt.subplots(2, 1, sharex=True, figsize=set_size())
    ax: 'plt.Axes'
    ax2: 'plt.Axes'
    for ax_ in (ax, ax2):
        # For visualize previous service quality
        ax_.plot(
            [idx[0] - 1, idx[0]], online_acc[:1] * 2, color='blue', marker='o', markersize=5, zorder=-1
        )
        ax_.plot(
            idx, online_acc,
            label='Online Service Quality', color='blue', marker='o', markersize=5, zorder=-1)
        # For visualize new service quality
        ax_.plot(
            [idx[-1], idx[-1] + 1], online_acc[-1:] * 2, '--', color='blue', marker='o', markersize=5, zorder=-1
        )
        # ax_.scatter(previous_idx, previous_acc, color='blue', s=10, label='Previous Service')
        ax_.scatter(deploy_idx, deploy_acc, color='green', s=20, label='New Deployed Service', zorder=1)
        ax_.scatter(not_deploy_idx, not_deploy_acc, color='gray', s=20, label='Failed Deployed Service', zorder=1)

    # hide the spines between ax and ax2
    ax.spines['bottom'].set_visible(False)
    ax2.spines['top'].set_visible(False)

    ax2.set_xticks(list(set(idx)))
    ax2.set_xticklabels(labels)
    ax.xaxis.set_visible(False)
    ax2.tick_params(top=False)  # don't put tick at the top

    ax.set_xlim(-0.5, 3.5)
    ax.set_ylim(0.895, 1.005)
    ax2.set_xlim(-0.5, 3.5)
    ax2.set_ylim(0.645, 0.755)
    ax.set_yticks([0.9, 0.95, 1.0])
    ax2.set_yticks([0.65, 0.7, 0.75])
    # Dummy subgraph label to get the correct spacing
    ax.set_ylabel('Accuracy', color='white')
    fig.text(0.04, 0.5, 'Accuracy', va='center', rotation='vertical')
    ax2.set_xlabel('Time')
    ax2.legend(loc='lower right')

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
