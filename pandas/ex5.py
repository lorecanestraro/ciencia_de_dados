import pandas as pd

df = pd.read_csv(
    "transacoes.csv",
    thousands="."
)

print(df)
