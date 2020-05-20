import pandas as pd

df = pd.read_csv("./DoubleCategories.csv")

df.drop_duplicates(subset=None, inplace=True)

df.to_csv("./Categories.csv")