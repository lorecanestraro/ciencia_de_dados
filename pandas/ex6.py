import pandas as pd

df = pd.read_csv(
    "sensores.csv",
    na_values=["NA", "-"]
)

print(df)