#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: May 26, 2023
"""
import matplotlib as mpl
from matplotlib.backends.backend_pgf import FigureCanvasPgf
from matplotlib import pyplot as plt

WIDTH = 506.295
COLOR_LIST = ['#fe4a49', '#2ab7ca', '#fed766', '#00CD66', '#ED790C']


def set_style():
    plt.style.use('classic')

    nice_fonts = {
        # Use LaTeX to write all text
        "text.usetex": True,
        "font.family": "serif",
        # Use 10pt font in plots, to match 10pt font in document
        "axes.labelsize": 16,
        "font.size": 16,
        # Make the legend/label fonts a little smaller
        "legend.fontsize": 12,
        "xtick.labelsize": 16,
        "ytick.labelsize": 16,
    }

    mpl.rcParams.update(nice_fonts)


def set_size(width=WIDTH, fraction=1):
    """ Set aesthetic figure dimensions to avoid scaling in latex.
    Parameters
    ----------
    width: float
            Width in pts
    fraction: float
            Fraction of the width which you wish the figure to occupy
    Returns
    -------
    fig_dim: tuple
            Dimensions of figure in inches
    """
    # Width of figure
    fig_width_pt = width * fraction

    # Convert from pt to inches
    inches_per_pt = 1 / 72.27

    # Golden ratio to set aesthetic figure height
    golden_ratio = (5 ** .5 - 1) / 2

    # Figure width in inches
    fig_width_in = fig_width_pt * inches_per_pt
    # Figure height in inches
    fig_height_in = fig_width_in * golden_ratio

    fig_dim = (fig_width_in, fig_height_in)
    #     fig_dim = (fig_width_in, 2*fig_height_in)

    return fig_dim


mpl.backend_bases.register_backend('pdf', FigureCanvasPgf)
set_style()
