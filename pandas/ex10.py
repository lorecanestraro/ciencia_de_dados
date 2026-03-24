import pandas as pd

chunks = pd.read_csv("transacoes_grandes.csv", sep=";", chunksize=20)

for i, bloco in enumerate(chunks):
    print(f"\nBloco {i+1}")
    print("Número de linhas:", len(bloco))
    print(bloco.head(3))