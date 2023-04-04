#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Apr 04, 2023
"""
import string

import emoji
import pandas as pd
import unicodedata

DEFAULT_BACKGROUND_COLOR_SEQUENCE = [
    '#fff1f0',  # red
    '#fff7e6',  # orange
    '#f6ffed',  # green
    '#e6f7ff',  # blue
    '#fff0f6',  # megenta
    '#f9f0ff',  # purple
    '#fff2e8',  # volcano
    '#fffbe6',  # gold
    '#fcffe6',  # lime
    '#e6fffb',  # cyan
    '#e6f7ff',  # geekblue
]
DEFAULT_COLOR_SEQUENCE = [
    '#cf1322',  # red
    '#d46b08',  # orange
    '#389e0d',  # green
    '#096dd9',  # blue
    '#531dab',  # purple
    '#c41d7f',  # megenta
    '#d4380d',  # volcano
    '#d48806',  # gold
    '#7cb305',  # lime
    '#08979c',  # cyan
    '#1d39c4',  # geekblue
]
DEFAULT_BORDER_COLOR_SEQUENCE = [
    '#ffa39e',  # red
    '#ffd591',  # orange
    '#b7eb8f',  # green
    '#91d5ff',  # blue
    '#f759ab',  # purple
    '#ffadd2',  # megenta
    '#ffbb96',  # volcano
    '#ffe58f',  # gold
    '#eaff8f',  # lime
    '#87e8de',  # cyan
    '#adc6ff',  # geekblue
]


def visualize_text(texts: pd.Series):
    """Visualize text"""

    def style_text(text: str):
        text_format_template = "<span style='color:{color};background-color:{background_color};'>{text}</span>"
        # Detect special characters in text
        # 1. punctuation
        # 2. digits
        # 3. whitespace
        # 4. escape characters (todo)
        # 5. emoji
        # 6. dialectics
        # 7. ascii letters
        # 8. others

        text_styled = ''
        for i, char in enumerate(text):
            if char in string.punctuation:
                # punctuation
                text_styled += text_format_template.format(
                    color=DEFAULT_COLOR_SEQUENCE[0], background_color=DEFAULT_BACKGROUND_COLOR_SEQUENCE[0],
                    text=char
                )
            elif char in string.digits:
                # digits
                text_styled += text_format_template.format(
                    color=DEFAULT_COLOR_SEQUENCE[1], background_color=DEFAULT_BACKGROUND_COLOR_SEQUENCE[1],
                    text=char
                )
            elif char in string.whitespace:
                # white space
                text_styled += text_format_template.format(
                    background_color=DEFAULT_BACKGROUND_COLOR_SEQUENCE[2],
                    text=char
                )
            elif char in emoji.UNICODE_EMOJI:
                # emoji
                text_styled += text_format_template.format(
                    color=DEFAULT_COLOR_SEQUENCE[3], background_color=DEFAULT_BACKGROUND_COLOR_SEQUENCE[3],
                    text=char
                )
            elif not unicodedata.is_normalized('NFKD', char):
                # dialectics
                text_styled += text_format_template.format(
                    color=DEFAULT_COLOR_SEQUENCE[4], background_color=DEFAULT_BACKGROUND_COLOR_SEQUENCE[4],
                    text=char
                )
            elif char in string.ascii_letters:
                text_styled += char
            else:
                text_styled += text_format_template.format(
                    color=DEFAULT_COLOR_SEQUENCE[5], background_color=DEFAULT_BACKGROUND_COLOR_SEQUENCE[5],
                    text=char
                )
        return text_styled

    return texts.map(style_text)


def visualize_image(image):
    """Visualize image"""
    pass


def visualize_label(labels: pd.Series):
    """Visualize label"""
    # make color mapper
    label_set: list = labels.unique().tolist()
    label_set.sort()
    color_mapper = dict(zip(label_set, DEFAULT_COLOR_SEQUENCE))
    background_color_mapper = dict(zip(label_set, DEFAULT_BACKGROUND_COLOR_SEQUENCE))
    border_color_mapper = dict(zip(label_set, DEFAULT_BORDER_COLOR_SEQUENCE))
    return labels.map(
        lambda x:
        f"<span "
        f"style='color:{color_mapper[x]};background-color:{background_color_mapper[x]};"
        f"border-color:{border_color_mapper[x]};border:1px solid;border-radius:4px;padding-inline:7px;'>"
        f"{x}"
        f"</span>"
    )
