import json
import pandas as pd
import numpy as np
import matplotlib
import os
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import geopandas as gpd
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.neighbors import KNeighborsClassifier
from sklearn.linear_model import LinearRegression
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix, r2_score, mean_squared_error
import warnings
warnings.filterwarnings('ignore')

os.makedirs('outputs', exist_ok=True)

# ============================================================
# ETAPA 1 — Carregamento do Dataset
# ============================================================
df = pd.read_json('dados.json', encoding='utf-8')

print("=" * 60)
print("ETAPA 1 — Conjunto de Dados Carregado")
print("=" * 60)
print(f"Total de registros : {len(df)}")
print(f"Colunas            : {list(df.columns)}")
print(f"\nDistribuição por tipoPessoa:")
print(df['tipoPessoa'].value_counts())

# ============================================================
# ETAPA 2 — Pré-processamento (Encoding + Features)
# ============================================================
le_uf    = LabelEncoder()
le_org   = LabelEncoder()
le_label = LabelEncoder()

df['uf_enc']    = le_uf.fit_transform(df['siglaUFPessoa'])
df['org_enc']   = le_org.fit_transform(df['nomeOrgaoSuperior'])
df['label_enc'] = le_label.fit_transform(df['tipoPessoa'])

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

scaler    = StandardScaler()
X_train_s = scaler.fit_transform(X_train)
X_test_s  = scaler.transform(X_test)

print(f"\n{'='*60}")
print("ETAPA 3 — Reescalamento (StandardScaler)")
print(f"{'='*60}")
print(f"Média  (treino): {X_train_s.mean(axis=0).round(4)}")
print(f"Desvio (treino): {X_train_s.std(axis=0).round(4)}")

# ============================================================
# ETAPAS 4 & 5 — Busca de k + Validação Cruzada
# ============================================================
k_values        = list(range(1, 16, 2))
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
        results.append({'k': k, 'weights': w,
                        'mean_acc': cv_scores.mean(), 'std_acc': cv_scores.std()})
        print(f"{k:>4}  {w:>10}  {cv_scores.mean():>12.4f}  {cv_scores.std():>8.4f}")

best = max(results, key=lambda r: r['mean_acc'])
print(f"\n→ Melhor config: k={best['k']}, weights='{best['weights']}', "
      f"CV acc={best['mean_acc']:.4f} ± {best['std_acc']:.4f}")

# ============================================================
# ETAPA 6 — Avaliação do Melhor Modelo (kNN)
# ============================================================
best_knn = KNeighborsClassifier(n_neighbors=best['k'], weights=best['weights'])
best_knn.fit(X_train_s, y_train)
y_pred   = best_knn.predict(X_test_s)
test_acc = accuracy_score(y_test, y_pred)

print(f"\n{'='*60}")
print("ETAPA 6 — Melhor Modelo — Avaliação no Conjunto de Teste")
print(f"{'='*60}")
print(f"k={best['k']}, weights='{best['weights']}'")
print(f"Acurácia no teste : {test_acc:.4f}  ({test_acc*100:.1f}%)")
print("\nRelatório de Classificação:")
present_labels = sorted(set(y_test) | set(y_pred))
present_names  = le_label.inverse_transform(present_labels)
print(classification_report(y_test, y_pred,
                             labels=present_labels,
                             target_names=present_names,
                             zero_division=0))

# ============================================================
# ETAPA 7 — Regressão Linear Simples (valor_pago ~ mes_ano)
# ============================================================
print(f"\n{'='*60}")
print("ETAPA 7 — Regressão Linear Simples")
print(f"  Variável independente (X): mes_ano")
print(f"  Variável dependente   (y): valor_pago")
print(f"{'='*60}")

X_reg = df[['mes_ano']].values
y_reg = df['valor_pago'].values

X_reg_train, X_reg_test, y_reg_train, y_reg_test = train_test_split(
    X_reg, y_reg, test_size=0.30, random_state=42
)

reg = LinearRegression()
reg.fit(X_reg_train, y_reg_train)
y_reg_pred = reg.predict(X_reg_test)

r2   = r2_score(y_reg_test, y_reg_pred)
rmse = np.sqrt(mean_squared_error(y_reg_test, y_reg_pred))

print(f"Coeficiente angular (β₁) : {reg.coef_[0]:.4f}")
print(f"Intercepto          (β₀) : {reg.intercept_:.4f}")
print(f"R²                       : {r2:.4f}")
print(f"RMSE                     : {rmse:.4f}")
print()
print("Interpretação dos parâmetros:")
print(f"  β₀ = {reg.intercept_:.2f} → valor estimado de 'valor_pago' quando mes_ano = 0")
print(f"  β₁ = {reg.coef_[0]:.4f}  → variação média em 'valor_pago' a cada unidade de mes_ano")
print(f"  R² = {r2:.4f}  → o modelo explica {r2*100:.1f}% da variância de valor_pago")
if r2 < 0.5:
    print("  ⚠  R² distante de 1: relação linear entre mes_ano e valor_pago é fraca,")
    print("     sugerindo que outros fatores influenciam o valor pago.")

# ============================================================
# ETAPA 8 — Mapa do Brasil com distribuição geográfica por UF
# ============================================================
print(f"\n{'='*60}")
print("ETAPA 8 — Mapa Geográfico — Valor Médio Pago por UF")
print(f"{'='*60}")

# Agrega valor médio por UF
uf_stats = (df.groupby('siglaUFPessoa')['valor_pago']
              .mean()
              .reset_index()
              .rename(columns={'siglaUFPessoa': 'SIGLA', 'valor_pago': 'valor_medio'}))

# Coordenadas aproximadas dos centróides de cada UF
uf_coords = {
    'AC': (-70.5, -9.0),  'AL': (-36.6, -9.6),  'AM': (-64.7, -3.4),
    'AP': (-51.1,  1.4),  'BA': (-41.7,-12.5),  'CE': (-39.4, -5.2),
    'DF': (-47.9,-15.8),  'ES': (-40.3,-19.2),  'GO': (-49.3,-15.9),
    'MA': (-44.3, -4.9),  'MG': (-44.7,-18.1),  'MS': (-54.8,-20.5),
    'MT': (-55.9,-12.6),  'PA': (-52.3, -3.7),  'PB': (-36.8, -7.1),
    'PE': (-37.9, -8.3),  'PI': (-42.8, -7.6),  'PR': (-51.6,-24.7),
    'RJ': (-43.2,-22.9),  'RN': (-36.5, -5.8),  'RO': (-63.0,-10.8),
    'RR': (-61.4,  2.1),  'RS': (-53.1,-30.0),  'SC': (-50.5,-27.3),
    'SE': (-37.4,-10.6),  'SP': (-48.5,-22.2),  'TO': (-48.3,-10.2),
}

# Pontos de destaque (mesma estrutura do código do mapa fornecido)
highlight_cities = [
    ((-49.27, -25.43), "Curitiba"),
    ((-46.63, -23.55), "São Paulo"),
    ((-43.20, -22.90), "Rio de Janeiro"),
    ((-38.50, -12.97), "Salvador"),
    ((-34.88,  -8.05), "Recife"),
    ((-60.02,  -3.10), "Manaus"),
]
guarapuava = (-51.46, -25.39)

# Carrega shapefile do Brasil
url = ("https://raw.githubusercontent.com/codeforgermany/"
       "click_that_hood/main/public/data/brazil-states.geojson")
try:
    brazil = gpd.read_file(url)
    map_loaded = True
    print("Shapefile do Brasil carregado com sucesso.")
except Exception as e:
    map_loaded = False
    print(f"Aviso: não foi possível carregar o shapefile — {e}")

# ============================================================
# FIGURA 1 — kNN: acurácia CV + distribuição de classes
# ============================================================
fig1, axes1 = plt.subplots(1, 2, figsize=(13, 5))
fig1.suptitle("kNN — Análise de Desempenho por k e Ponderação",
              fontsize=14, fontweight='bold')

colors_knn = {'uniform': '#2196F3', 'distance': '#FF5722'}
for w in weights_options:
    sub   = [r for r in results if r['weights'] == w]
    ks    = [r['k']        for r in sub]
    means = [r['mean_acc'] for r in sub]
    stds  = [r['std_acc']  for r in sub]
    axes1[0].errorbar(ks, means, yerr=stds, marker='o', label=w,
                      color=colors_knn[w], capsize=4, linewidth=2)

axes1[0].axvline(x=best['k'], color='green', linestyle='--',
                 alpha=0.6, label=f"melhor k={best['k']}")
axes1[0].set_title("Acurácia CV (média ± desvio) por k")
axes1[0].set_xlabel("k (nº de vizinhos)")
axes1[0].set_ylabel("Acurácia média (CV=5)")
axes1[0].legend(title="weights")
axes1[0].set_xticks(k_values)
axes1[0].grid(True, alpha=0.3)

class_counts = df['tipoPessoa'].value_counts()
axes1[1].barh(class_counts.index, class_counts.values,
              color=['#2196F3','#FF5722','#4CAF50','#9C27B0','#FF9800'])
axes1[1].set_title("Distribuição das Classes (tipoPessoa)")
axes1[1].set_xlabel("Quantidade de registros")
for i, v in enumerate(class_counts.values):
    axes1[1].text(v + 0.1, i, str(v), va='center', fontsize=9)

plt.tight_layout()
plt.savefig('outputs/knn_results.png', dpi=150, bbox_inches='tight')
plt.close()
print("Gráfico kNN salvo em: outputs/knn_results.png")

# ============================================================
# FIGURA 2 — Regressão Linear
# ============================================================
fig2, axes2 = plt.subplots(1, 2, figsize=(13, 5))
fig2.suptitle("Regressão Linear Simples — valor_pago ~ mes_ano",
              fontsize=14, fontweight='bold')

# Dispersão + reta ajustada
x_line = np.linspace(X_reg_test.min(), X_reg_test.max(), 200).reshape(-1, 1)
y_line = reg.predict(x_line)
axes2[0].scatter(X_reg_test, y_reg_test, alpha=0.4, color='#2196F3',
                 label='Dados de teste', s=20)
axes2[0].plot(x_line, y_line, color='#FF5722', linewidth=2,
              label=f'Reta ajustada\nR²={r2:.4f}')
axes2[0].set_title("Dispersão e Reta de Regressão")
axes2[0].set_xlabel("mes_ano")
axes2[0].set_ylabel("valor_pago")
axes2[0].legend()
axes2[0].grid(True, alpha=0.3)

# Resíduos
residuals = y_reg_test - y_reg_pred
axes2[1].scatter(y_reg_pred, residuals, alpha=0.4, color='#9C27B0', s=20)
axes2[1].axhline(0, color='red', linestyle='--', linewidth=1.5)
axes2[1].set_title("Resíduos vs Valores Preditos")
axes2[1].set_xlabel("Valores preditos")
axes2[1].set_ylabel("Resíduos")
axes2[1].grid(True, alpha=0.3)

plt.tight_layout()
plt.savefig('outputs/regressao_linear.png', dpi=150, bbox_inches='tight')
plt.close()
print("Gráfico de regressão salvo em: outputs/regressao_linear.png")

# ============================================================
# FIGURA 3 — Mapa do Brasil
# ============================================================
fig3, ax3 = plt.subplots(figsize=(12, 12))

if map_loaded:
    brazil.plot(ax=ax3, color='#f5f0e8', edgecolor='#555555', linewidth=0.8)

# Plota valor médio por UF como bolhas
if not uf_stats.empty:
    vmin = uf_stats['valor_medio'].min()
    vmax = uf_stats['valor_medio'].max()
    for _, row in uf_stats.iterrows():
        sigla = row['SIGLA']
        if sigla in uf_coords:
            lon, lat = uf_coords[sigla]
            # tamanho proporcional ao valor médio
            size = 40 + 300 * (row['valor_medio'] - vmin) / max(vmax - vmin, 1)
            ax3.scatter(lon, lat,
                        s=size,
                        c=row['valor_medio'],
                        cmap='YlOrRd',
                        vmin=vmin, vmax=vmax,
                        alpha=0.8, zorder=4,
                        edgecolors='#333333', linewidths=0.5)
            ax3.text(lon, lat - 0.8, sigla,
                     ha='center', fontsize=6.5, color='#222222', zorder=5)

# Cidades de destaque (estrutura do código do mapa fornecido)
city_colors  = ['#2196F3','#FF5722','#4CAF50','#9C27B0','#FF9800','#00BCD4']
city_markers = ['o','s','^','D','P','h']
for idx, ((lon, lat), city) in enumerate(highlight_cities):
    ax3.scatter(lon, lat,
                color=city_colors[idx % len(city_colors)],
                marker=city_markers[idx % len(city_markers)],
                s=100, zorder=6, label=city,
                edgecolors='white', linewidths=0.8)
    ax3.text(lon + 0.5, lat + 0.5, city, fontsize=9, zorder=7)

# Guarapuava — destaque especial (estrela preta)
ax3.scatter(*guarapuava, color='black', s=250, marker='*', zorder=10)
ax3.text(guarapuava[0] - 10.5, guarapuava[1],
         "Guarapuava", fontsize=12, fontweight='bold', color='black', zorder=11)

ax3.set_title("Distribuição Geográfica — Valor Médio Pago por UF\n"
              "(tamanho e cor das bolhas proporcionais ao valor médio)",
              fontsize=13, fontweight='bold')
ax3.set_xlabel("Longitude")
ax3.set_ylabel("Latitude")
ax3.legend(title="Cidades destaque", loc='lower left', fontsize=8)
ax3.grid(True, alpha=0.25)

plt.tight_layout()
plt.savefig('outputs/mapa_brasil.png', dpi=150, bbox_inches='tight')
plt.close()
print("Mapa salvo em: outputs/mapa_brasil.png")

# ============================================================
# ETAPA 9 — Reflexão Final
# ============================================================
print(f"\n{'='*60}")
print("ETAPA 9 — Reflexão Final")
print(f"{'='*60}")
print(f"[kNN]")
print(f"  Melhor k          : {best['k']}")
print(f"  Melhor weights    : {best['weights']}")
print(f"  CV acc (treino)   : {best['mean_acc']*100:.1f}%")
print(f"  Acurácia (teste)  : {test_acc*100:.1f}%")
print(f"\n[Regressão Linear]")
print(f"  β₀ (intercepto)   : {reg.intercept_:.4f}")
print(f"  β₁ (coeficiente)  : {reg.coef_[0]:.4f}")
print(f"  R²                : {r2:.4f}")
print(f"  RMSE              : {rmse:.4f}")
print(f"\n[Saídas geradas]")
print(f"  outputs/knn_results.png")
print(f"  outputs/regressao_linear.png")
print(f"  outputs/mapa_brasil.png")