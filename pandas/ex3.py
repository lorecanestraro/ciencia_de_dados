import pandas as pd

df = pd.read_csv(
    "log_sistema.csv",
    comment="#",
    engine="python",
    nrows=3
)

print(df)