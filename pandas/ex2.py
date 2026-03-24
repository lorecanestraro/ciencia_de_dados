import pandas as pd

df = pd.read_csv("clima.csv")
df["data"] = pd.to_datetime(df["data"])
df.set_index("data", inplace=True)

print(df.info())