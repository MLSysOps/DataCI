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

RED_BACKGROUND_COLOR = '#fff1f0'
RED_COLOR = '#cf1322'
RED_BORDER_COLOR = '#ffa39e'
ORANGE_BACKGROUND_COLOR = '#fff7e6'
ORANGE_COLOR = '#d46b08'
ORANGE_BORDER_COLOR = '#ffd591'
GREEN_BACKGROUND_COLOR = '#f6ffed'
GREEN_COLOR = '#389e0d'
GREEN_BORDER_COLOR = '#b7eb8f'
BLUE_BACKGROUND_COLOR = '#e6f7ff'
BLUE_COLOR = '#096dd9'
BLUE_BORDER_COLOR = '#91d5ff'
MEGENTA_BACKGROUND_COLOR = '#fff0f6'
MEGENTA_COLOR = '#531dab'
MEGENTA_BORDER_COLOR = '#f759ab'
PURPLE_BACKGROUND_COLOR = '#f9f0ff'
PURPLE_COLOR = '#c41d7f'
PURPLE_BORDER_COLOR = '#ffadd2'
VOLCANO_BACKGROUND_COLOR = '#fff2e8'
VOLCANO_COLOR = '#d4380d'
VOLCANO_BORDER_COLOR = '#ffbb96'
GOLD_BACKGROUND_COLOR = '#fffbe6'
GOLD_COLOR = '#d48806'
GOLD_BORDER_COLOR = '#ffe58f'
LIME_BACKGROUND_COLOR = '#fcffe6'
LIME_COLOR = '#7cb305'
LIME_BORDER_COLOR = '#eaff8f'
CYAN_BACKGROUND_COLOR = '#e6fffb'
CYAN_COLOR = '#08979c'
CYAN_BORDER_COLOR = '#87e8de'
GEEKBLUE_BACKGROUND_COLOR = '#e6f7ff'
GEEKBLUE_COLOR = '#1d39c4'
GEEKBLUE_BORDER_COLOR = '#adc6ff'

DEFAULT_BACKGROUND_COLOR_SEQUENCE = [
    RED_BACKGROUND_COLOR,
    ORANGE_BACKGROUND_COLOR,
    GREEN_BACKGROUND_COLOR,
    BLUE_BACKGROUND_COLOR,
    MEGENTA_BACKGROUND_COLOR,
    PURPLE_BACKGROUND_COLOR,
    VOLCANO_BACKGROUND_COLOR,
    GOLD_BACKGROUND_COLOR,
    LIME_BACKGROUND_COLOR,
    CYAN_BACKGROUND_COLOR,
    GEEKBLUE_BACKGROUND_COLOR,
]
DEFAULT_COLOR_SEQUENCE = [
    RED_COLOR,
    ORANGE_COLOR,
    GREEN_COLOR,
    BLUE_COLOR,
    MEGENTA_COLOR,
    PURPLE_COLOR,
    VOLCANO_COLOR,
    GOLD_COLOR,
    LIME_COLOR,
    CYAN_COLOR,
    GEEKBLUE_COLOR,
]

DEFAULT_BORDER_COLOR_SEQUENCE = [
    RED_BORDER_COLOR,
    ORANGE_BORDER_COLOR,
    GREEN_BORDER_COLOR,
    BLUE_BORDER_COLOR,
    MEGENTA_BORDER_COLOR,
    PURPLE_BORDER_COLOR,
    VOLCANO_BORDER_COLOR,
    GOLD_BORDER_COLOR,
    LIME_BORDER_COLOR,
    CYAN_BORDER_COLOR,
    GEEKBLUE_BORDER_COLOR,
]


def visualize_text(texts: pd.Series):
    """Visualize text"""

    def style_text(text: str):
        text_format_template = "<span style='color:{color};background-color:{background_color};'>{text}</span>"
        text_uncolorable_format_template = "<span style='background-color:{background_color};" \
                                           "border:2px solid;border-color:{border_color}'>{text}</span>"
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
                    color=ORANGE_COLOR, background_color=ORANGE_BACKGROUND_COLOR,
                    text=char
                )
            elif char in string.digits:
                # digits
                text_styled += text_format_template.format(
                    color=MEGENTA_COLOR, background_color=MEGENTA_BACKGROUND_COLOR,
                    text=char
                )
            elif char in string.whitespace:
                # white space
                text_styled += text_uncolorable_format_template.format(
                    background_color=BLUE_BACKGROUND_COLOR, border_color=BLUE_BORDER_COLOR,
                    text=char
                )
            elif char in emoji.UNICODE_EMOJI_ENGLISH:
                # emoji
                text_styled += text_uncolorable_format_template.format(
                    background_color=PURPLE_BACKGROUND_COLOR, border_color=PURPLE_BORDER_COLOR,
                    text=char
                )
            elif not unicodedata.is_normalized('NFKD', char):
                # dialectics
                text_styled += text_format_template.format(
                    color=VOLCANO_COLOR, background_color=VOLCANO_BACKGROUND_COLOR,
                    text=char
                )
            elif char in string.ascii_letters:
                text_styled += char
            else:
                text_styled += text_format_template.format(
                    color=CYAN_COLOR, background_color=CYAN_BACKGROUND_COLOR,
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
