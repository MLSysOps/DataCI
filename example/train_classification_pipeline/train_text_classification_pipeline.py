import augly.text as txtaugs
import pandas as pd
import unicodedata
from cleantext import clean

from dataci.pipeline import Stage


class TextClean(Stage):
    @staticmethod
    def clean(text):
        # remove emoji, space, and to lower case
        text = clean(text, to_ascii=False, lower=True, normalize_whitespace=True, no_emoji=True)

        # remove accent
        text = ''.join(c for c in unicodedata.normalize('NFD', text) if unicodedata.category(c) != 'Mn')

        return text

    def run(self, inputs):
        df = pd.read_csv(inputs / '202211_pairwise.csv')
        df['to_product_name'] = df['to_product_name'].map(self.clean)
        return df


class TextAugmentation(Stage):
    def run(self, inputs):
        df = pd.read_csv(inputs)
        transform = txtaugs.InsertPunctuationChars(
            granularity="all",
            cadence=5.0,
            vary_chars=True,
        )
        df['to_product_name'] = df['to_product_name'].map(transform)
        return df


text_clean = TextClean('text_clean', inputs='pairwise_raw[train]', outputs='text_clean.csv')
text_augmentation = TextAugmentation('text_augmentation', inputs='text_clean.csv', outputs='text_aug.csv')
print('Running text clean')
text_clean()
text_augmentation()
