import pandas as pd

chunks = pd.read_csv(
    "dados_sensor_gigante.csv",
    na_values=["NA", "-"],
    chunksize=10
)

for i, bloco in enumerate(chunks):
    print(f"\nBloco {i+1}")
    
    media_temp = bloco["temperatura"].mean()
    
    faltantes = bloco["temperatura"].isna().sum()
    
    print("Temperatura média:", media_temp)
    print("Valores ausentes em temperatura:", faltantes)