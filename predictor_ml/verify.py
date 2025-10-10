import pandas as pd

df = pd.read_parquet("data/processed_games.parquet")
print(df.head())
print(df.info())
print(df.describe())
print(df["label_home_win"].value_counts())
