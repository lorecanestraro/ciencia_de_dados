import json
import pandas as pd
import numpy as np
import matplotlib
import os
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.neighbors import KNeighborsClassifier
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix
import warnings
warnings.filterwarnings('ignore')


df = pd.read_json('dados.json', encoding='utf-8')

print("=" * 60)
print("ETAPA 1 — Conjunto de Dados Carregado")
print("=" * 60)
print(f"Total de registros : {len(df)}")
print(f"Colunas            : {list(df.columns)}")
print(f"\nDistribuição por tipoPessoa:")
print(df['tipoPessoa'].value_counts())


le_uf    = LabelEncoder()
le_org   = LabelEncoder()
le_label = LabelEncoder()

df['uf_enc']     = le_uf.fit_transform(df['siglaUFPessoa'])
df['org_enc']    = le_org.fit_transform(df['nomeOrgaoSuperior'])
df['label_enc']  = le_label.fit_transform(df['tipoPessoa'])

X = df[['valor_pago', 'mes_ano', 'uf_enc', 'org_enc']].values
y = df['label_enc'].values


X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.30, random_state=42, stratify=None
)
print(f"\n{'='*60}")
print("ETAPA 2 — Divisão Treino/Teste (70/30)")
print(f"{'='*60}")
print(f"Treino : {len(X_train)} amostras")
print(f"Teste  : {len(X_test)} amostras")

scaler   = StandardScaler()
X_train_s = scaler.fit_transform(X_train)
X_test_s  = scaler.transform(X_test)

print(f"\n{'='*60}")
print("ETAPA 3 — Reescalamento (StandardScaler)")
print(f"{'='*60}")
print(f"Média  (treino): {X_train_s.mean(axis=0).round(4)}")
print(f"Desvio (treino): {X_train_s.std(axis=0).round(4)}")


k_values = list(range(1, 16, 2))   # ímpares: 1,3,5,7,9,11,13,15
weights_options = ['uniform', 'distance']

print(f"\n{'='*60}")
print("ETAPAS 4 & 5 — Busca de k + Validação Cruzada (cv=5)")
print(f"{'='*60}")
print(f"{'k':>4}  {'weights':>10}  {'CV mean acc':>12}  {'CV std':>8}")
print("-" * 42)

results = []
for w in weights_options:
    for k in k_values:
        knn = KNeighborsClassifier(n_neighbors=k, weights=w)
        cv_scores = cross_val_score(knn, X_train_s, y_train, cv=5, scoring='accuracy')
        results.append({
            'k': k, 'weights': w,
            'mean_acc': cv_scores.mean(),
            'std_acc':  cv_scores.std()
        })
        print(f"{k:>4}  {w:>10}  {cv_scores.mean():>12.4f}  {cv_scores.std():>8.4f}")

# Melhor configuração
best = max(results, key=lambda r: r['mean_acc'])
print(f"\n→ Melhor config: k={best['k']}, weights='{best['weights']}', "
      f"CV acc={best['mean_acc']:.4f} ± {best['std_acc']:.4f}")


best_knn = KNeighborsClassifier(n_neighbors=best['k'], weights=best['weights'])
best_knn.fit(X_train_s, y_train)
y_pred = best_knn.predict(X_test_s)
test_acc = accuracy_score(y_test, y_pred)

print(f"\n{'='*60}")
print("ETAPA 6 — Melhor Modelo — Avaliação no Conjunto de Teste")
print(f"{'='*60}")
print(f"k={best['k']}, weights='{best['weights']}'")
print(f"Acurácia no teste : {test_acc:.4f}  ({test_acc*100:.1f}%)")
print("\nRelatório de Classificação:")
target_names = le_label.classes_
present_labels = sorted(set(y_test) | set(y_pred))
present_names  = le_label.inverse_transform(present_labels)
print(classification_report(y_test, y_pred, labels=present_labels, target_names=present_names, zero_division=0))


fig, axes = plt.subplots(1, 2, figsize=(13, 5))
fig.suptitle("kNN — Análise de Desempenho por k e Ponderação", fontsize=14, fontweight='bold')

colors = {'uniform': '#2196F3', 'distance': '#FF5722'}
for w in weights_options:
    sub = [r for r in results if r['weights'] == w]
    ks    = [r['k']        for r in sub]
    means = [r['mean_acc'] for r in sub]
    stds  = [r['std_acc']  for r in sub]
    axes[0].errorbar(ks, means, yerr=stds, marker='o', label=w,
                     color=colors[w], capsize=4, linewidth=2)

axes[0].set_title("Acurácia CV (média ± desvio) por k")
axes[0].set_xlabel("k (nº de vizinhos)")
axes[0].set_ylabel("Acurácia média (CV=5)")
axes[0].legend(title="weights")
axes[0].set_xticks(k_values)
axes[0].grid(True, alpha=0.3)
axes[0].axvline(x=best['k'], color='green', linestyle='--', alpha=0.6, label=f"melhor k={best['k']}")

# Distribuição das classes no dataset
class_counts = df['tipoPessoa'].value_counts()
axes[1].barh(class_counts.index, class_counts.values,
             color=['#2196F3','#FF5722','#4CAF50','#9C27B0','#FF9800'])
axes[1].set_title("Distribuição das Classes (tipoPessoa)")
axes[1].set_xlabel("Quantidade de registros")
for i, v in enumerate(class_counts.values):
    axes[1].text(v + 0.1, i, str(v), va='center', fontsize=9)

os.makedirs('outputs', exist_ok=True)

plt.tight_layout()

plt.savefig(
    'outputs/knn_results.png',
    dpi=150,
    bbox_inches='tight'
)

plt.close()

print("\nGráfico salvo em: outputs/knn_results.png")


print(f"\n{'='*60}")
print("ETAPA 7 — Reflexão")
print(f"{'='*60}")
print(f"Melhor k          : {best['k']}")
print(f"Melhor weights    : {best['weights']}")
print(f"CV acc (treino)   : {best['mean_acc']*100:.1f}%")
print(f"Acurácia (teste)  : {test_acc*100:.1f}%")
print("""
Observações:
• O reescalamento foi essencial: 'valor' varia de −19 295 a 5 490 376,
  enquanto 'anoMes' tem escala ~201 600. Sem padronização, 'valor'
  dominaria completamente o cálculo de distância.
• Com apenas 32 amostras e 5 classes desbalanceadas, o kNN tende
  a superajustar para k pequeno.
• A ponderação por distância (weights='distance') beneficia registros
  próximos mais do que os distantes, sendo geralmente superior em
  conjuntos pequenos.
""")