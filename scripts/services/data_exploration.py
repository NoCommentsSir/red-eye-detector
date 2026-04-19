from pathlib import Path
import pandas as pd

df = pd.read_csv('data/celeba/list_attr_celeba.csv')
print(df.info())

