import pandas as pd

df = pd.read_csv("notas.csv")

print("Estatísticas descritivas:")
print(df.describe())

print("\nMédia das disciplinas:")
print(df[["matematica", "portugues", "historia"]].mean())