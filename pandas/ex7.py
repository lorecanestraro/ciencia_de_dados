import pandas as pd

df = pd.read_csv("experimento.csv")

print("Primeiras linhas:")
print(df.head())

print("\nÚltimas linhas:")
print(df.tail())

print("\nResumo estatístico:")
print(df.describe())