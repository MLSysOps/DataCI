from dataci.pipeline import Stage
import pandas as pd

class TextAugmentation(Stage):
    def run(self):
        df = pd.read_csv(self.inputs / '202211_pairwise.csv')
        print('text aug...')
        print(df)


text_augmentation = TextAugmentation('text_augmentation', inputs='pairwise_raw[train]', outputs=None)
text_augmentation.run()
