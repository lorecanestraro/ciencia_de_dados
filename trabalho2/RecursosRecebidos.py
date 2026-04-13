"""
Pipeline de Dados Governamentais - Gastos Públicos do Brasil
Fonte: API do Portal da Transparência do Governo Federal
Requisitos: pip install requests pandas matplotlib seaborn sqlalchemy
"""

import requests
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import seaborn as sns
import sqlite3
import json
import os
from datetime import datetime


API_KEY = "chave_api_aqui"  
BASE_URL = "https://api.portaldatransparencia.gov.br/api-de-dados"
DB_PATH = "gastos_publicos.db"
JSON_PATH = "gastos_publicos.json"

HEADERS = {
    "chave-api-dados": API_KEY,
    "Accept": "application/json"
}


ANO_INICIO = datetime.now().year - 10
ANO_FIM = datetime.now().year - 1  



def coletar_despesas_por_ano(ano: int, paginas: int = 3) -> list:
    """Coleta despesas de um determinado ano, múltiplas páginas."""
    todos_registros = []
    mes_inicio = f"01/{ano}"
    mes_fim = f"12/{ano}"

    for pagina in range(1, paginas + 1):
        params = {
            "mesAnoInicio": mes_inicio,
            "mesAnoFim": mes_fim,
            "pagina": pagina
        }

        try:
            response = requests.get(
                f"{BASE_URL}/despesas/recursos-recebidos",
                headers=HEADERS,
                params=params,
                timeout=30
            )

            if response.status_code == 200:
                dados = response.json()
                if not dados:
                    break
                todos_registros.extend(dados)
                print(f"  Ano {ano} | Página {pagina}: {len(dados)} registros coletados")
            else:
                print(f"  Erro {response.status_code} ao coletar ano {ano}, página {pagina}")
                break

        except requests.exceptions.RequestException as e:
            print(f"  Erro de conexão: {e}")
            break

    return todos_registros


def coletar_todos_dados() -> list:
    """Coleta dados dos últimos 10 anos."""
    print(f"\n{'='*60}")
    print(f"COLETANDO DADOS: {ANO_INICIO} a {ANO_FIM}")
    print(f"{'='*60}")

    todos = []
    for ano in range(ANO_INICIO, ANO_FIM + 1):
        print(f"\nProcessando ano {ano}...")
        registros = coletar_despesas_por_ano(ano, paginas=2)
        todos.extend(registros)
        print(f"  Total acumulado: {len(todos)} registros")

    print(f"\nColeta concluída! Total: {len(todos)} registros")
    return todos



def tratar_dados(registros: list) -> pd.DataFrame:
    """Trata e converte os dados coletados."""
    print(f"\n{'='*60}")
    print("TRATANDO DADOS")
    print(f"{'='*60}")

    df = pd.DataFrame(registros)

    if df.empty:
        print("Nenhum dado para tratar.")
        return df

    print(f"Colunas disponíveis: {list(df.columns)}")

    # Renomear colunas conforme retorno da API
    colunas_map = {
        "codigoOrgao": "cod_orgao",
        "nomeOrgao": "orgao",
        "valorEmpenhado": "valor_empenhado",
        "valorLiquidado": "valor_liquidado",
        "valorPago": "valor_pago",
        "mesAno": "mes_ano",
        "nomeFavorecido": "favorecido",
        "cpfCnpjFavorecido": "cpf_cnpj",
    }
    df = df.rename(columns={k: v for k, v in colunas_map.items() if k in df.columns})

    # Converter valores monetários para float
    for col in ["valor_empenhado", "valor_liquidado", "valor_pago"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    # Extrair ano e mês
    if "mes_ano" in df.columns:
        df["mes_ano"] = df["mes_ano"].astype(str)
        df["ano"] = df["mes_ano"].str[-4:].astype(int, errors="ignore")
        df["mes"] = df["mes_ano"].str[:2].astype(int, errors="ignore")

    # Remover duplicatas
    df = df.drop_duplicates()

    print(f"Registros após tratamento: {len(df)}")
    print(f"Período: {df['ano'].min()} a {df['ano'].max()}" if "ano" in df.columns else "")

    # Salvar como JSON (requisito do trabalho)
    salvar_json(df)

    return df


def salvar_json(df: pd.DataFrame):
    """Salva os dados em formato JSON."""
    df.to_json(JSON_PATH, orient="records", force_ascii=False, indent=2)
    print(f"Dados salvos em JSON: {JSON_PATH}")



def calcular_estatisticas(df: pd.DataFrame) -> pd.DataFrame:
    """Calcula média, máximo e mínimo dos gastos por ano."""
    print(f"\n{'='*60}")
    print("CALCULANDO ESTATÍSTICAS")
    print(f"{'='*60}")

    if "ano" not in df.columns or "valor_pago" not in df.columns:
        print("Colunas necessárias não encontradas.")
        return pd.DataFrame()

    stats = df.groupby("ano")["valor_pago"].agg(
        total="sum",
        media="mean",
        maximo="max",
        minimo="min",
        contagem="count"
    ).reset_index()

    print("\nEstatísticas por ano (valor pago em R$):")
    print(stats.to_string(index=False))

    return stats



def formatar_reais(valor, _):
    """Formata valores em bilhões/milhões de reais."""
    if abs(valor) >= 1e9:
        return f"R$ {valor/1e9:.1f}B"
    elif abs(valor) >= 1e6:
        return f"R$ {valor/1e6:.1f}M"
    return f"R$ {valor:,.0f}"


def grafico_evolucao_anual(stats: pd.DataFrame):
    """Gráfico de evolução do total de gastos por ano."""
    fig, ax = plt.subplots(figsize=(12, 6))

    bars = ax.bar(stats["ano"], stats["total"], color="#1a6bb0", edgecolor="white", linewidth=0.5)

   
    for bar, val in zip(bars, stats["total"]):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() * 1.01,
                formatar_reais(val, None), ha="center", va="bottom", fontsize=8, fontweight="bold")

    ax.set_title("Evolução dos Gastos Públicos Federais\n(últimos 10 anos)", fontsize=14, fontweight="bold")
    ax.set_xlabel("Ano", fontsize=11)
    ax.set_ylabel("Total Pago (R$)", fontsize=11)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(formatar_reais))
    ax.set_xticks(stats["ano"])
    ax.grid(axis="y", linestyle="--", alpha=0.5)
    plt.tight_layout()
    plt.savefig("grafico_evolucao_anual.png", dpi=150)
    plt.show()
    print("Gráfico salvo: grafico_evolucao_anual.png")


def grafico_media_max_min(stats: pd.DataFrame):
    """Gráfico adicional: comparativo de média, máximo e mínimo por ano."""
    fig, ax = plt.subplots(figsize=(12, 6))

    x = stats["ano"]
    ax.plot(x, stats["media"], marker="o", label="Média", color="#2196F3", linewidth=2)
    ax.plot(x, stats["maximo"], marker="^", label="Máximo", color="#F44336", linewidth=2)
    ax.plot(x, stats["minimo"], marker="v", label="Mínimo", color="#4CAF50", linewidth=2)

    ax.fill_between(x, stats["minimo"], stats["maximo"], alpha=0.1, color="#2196F3")

    ax.set_title("Estatísticas de Gastos Públicos por Ano\n(Média, Máximo e Mínimo)", fontsize=14, fontweight="bold")
    ax.set_xlabel("Ano", fontsize=11)
    ax.set_ylabel("Valor (R$)", fontsize=11)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(formatar_reais))
    ax.set_xticks(stats["ano"])
    ax.legend()
    ax.grid(linestyle="--", alpha=0.4)
    plt.tight_layout()
    plt.savefig("grafico_media_max_min.png", dpi=150)
    plt.show()
    print("Gráfico salvo: grafico_media_max_min.png")


def grafico_top_orgaos(df: pd.DataFrame):
    """Gráfico adicional: top 10 órgãos com maiores gastos."""
    if "orgao" not in df.columns:
        print("Coluna 'orgao' não encontrada.")
        return

    top = df.groupby("orgao")["valor_pago"].sum().nlargest(10).reset_index()

    fig, ax = plt.subplots(figsize=(12, 7))
    colors = sns.color_palette("Blues_d", len(top))
    bars = ax.barh(top["orgao"], top["valor_pago"], color=colors)

    for bar, val in zip(bars, top["valor_pago"]):
        ax.text(val * 1.01, bar.get_y() + bar.get_height()/2,
                formatar_reais(val, None), va="center", fontsize=8)

    ax.set_title("Top 10 Órgãos com Maiores Gastos\n(últimos 10 anos)", fontsize=14, fontweight="bold")
    ax.set_xlabel("Total Pago (R$)", fontsize=11)
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(formatar_reais))
    ax.invert_yaxis()
    ax.grid(axis="x", linestyle="--", alpha=0.4)
    plt.tight_layout()
    plt.savefig("grafico_top_orgaos.png", dpi=150)
    plt.show()
    print("Gráfico salvo: grafico_top_orgaos.png")



def criar_schema(conn: sqlite3.Connection):
    """
    Cria as tabelas com schema explícito e índices para consultas rápidas.
    Usar CREATE TABLE IF NOT EXISTS garante idempotência: rodar o pipeline
    múltiplas vezes não duplica a estrutura, apenas os dados (tratados via
    DROP TABLE na etapa de inserção com if_exists='replace').
    """
    cursor = conn.cursor()

   
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS despesas (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            ano             INTEGER,
            mes             INTEGER,
            cod_orgao       TEXT,
            orgao           TEXT,
            favorecido      TEXT,
            cpf_cnpj        TEXT,
            valor_empenhado REAL DEFAULT 0.0,
            valor_liquidado REAL DEFAULT 0.0,
            valor_pago      REAL DEFAULT 0.0,
            inserido_em     TEXT DEFAULT (datetime('now'))
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS estatisticas_anuais (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            ano       INTEGER UNIQUE,
            total     REAL,
            media     REAL,
            maximo    REAL,
            minimo    REAL,
            contagem  INTEGER
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS top_orgaos (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            orgao       TEXT UNIQUE,
            total_pago  REAL,
            ranking     INTEGER
        )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_despesas_ano   ON despesas (ano)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_despesas_orgao ON despesas (orgao)")

    conn.commit()
    print("  Schema criado (tabelas + índices)")


def inserir_despesas(conn: sqlite3.Connection, df: pd.DataFrame):
    """
    Insere o DataFrame de despesas na tabela 'despesas'.
    if_exists='replace' apaga e recria a tabela antes de inserir,
    garantindo que re-execuções do pipeline não gerem duplicatas.
    Após o to_sql, recria os índices pois o replace os remove.
    """
    colunas_db = [
        "ano", "mes", "cod_orgao", "orgao", "favorecido",
        "cpf_cnpj", "valor_empenhado", "valor_liquidado", "valor_pago"
    ]
    
    colunas_presentes = [c for c in colunas_db if c in df.columns]
    df_inserir = df[colunas_presentes].copy()

    df_inserir.to_sql("despesas", conn, if_exists="replace", index=False)

    
    cursor = conn.cursor()
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_despesas_ano   ON despesas (ano)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_despesas_orgao ON despesas (orgao)")
    conn.commit()

    print(f"  Tabela 'despesas': {len(df_inserir)} registros inseridos")


def inserir_estatisticas(conn: sqlite3.Connection, stats: pd.DataFrame):
    """Insere as estatísticas anuais calculadas."""
    if stats.empty:
        print("  Nenhuma estatística para inserir.")
        return

    stats.to_sql("estatisticas_anuais", conn, if_exists="replace", index=False)
    conn.commit()
    print(f"  Tabela 'estatisticas_anuais': {len(stats)} registros inseridos")


def inserir_top_orgaos(conn: sqlite3.Connection, df: pd.DataFrame):
    """Calcula e persiste o ranking dos top 10 órgãos por total gasto."""
    if "orgao" not in df.columns or "valor_pago" not in df.columns:
        print("  Colunas para ranking não encontradas. Pulando top_orgaos.")
        return

    top = (
        df.groupby("orgao")["valor_pago"]
        .sum()
        .nlargest(10)
        .reset_index()
        .rename(columns={"valor_pago": "total_pago"})
    )
    top["ranking"] = range(1, len(top) + 1)

    top.to_sql("top_orgaos", conn, if_exists="replace", index=False)
    conn.commit()
    print(f"  Tabela 'top_orgaos': {len(top)} registros inseridos")


def verificar_banco(conn: sqlite3.Connection):
    """
    Executa SELECTs de verificação em cada tabela e imprime
    um resumo para confirmar que os dados foram gravados corretamente.
    """
    cursor = conn.cursor()

    print("\n  --- Verificação do banco ---")

    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    tabelas = [row[0] for row in cursor.fetchall()]
    print(f"  Tabelas: {tabelas}")

    
    for tabela in tabelas:
        cursor.execute(f"SELECT COUNT(*) FROM {tabela}")
        total = cursor.fetchone()[0]
        print(f"  {tabela}: {total} registros")

    if "despesas" in tabelas:
        cursor.execute("SELECT DISTINCT ano FROM despesas ORDER BY ano")
        anos = [row[0] for row in cursor.fetchall()]
        print(f"  Anos em 'despesas': {anos}")

    if "top_orgaos" in tabelas:
        cursor.execute("SELECT ranking, orgao, total_pago FROM top_orgaos ORDER BY ranking LIMIT 3")
        print("  Top 3 órgãos (do banco):")
        for row in cursor.fetchall():
            print(f"    #{row[0]} {row[1]}: R$ {row[2]:,.0f}")


def salvar_banco_dados(df: pd.DataFrame, stats: pd.DataFrame):
    """
    Orquestra toda a etapa de banco de dados:
      1. Abre conexão com o arquivo .db
      2. Cria schema (tabelas + índices)
      3. Insere despesas, estatísticas e ranking
      4. Verifica a gravação com SELECTs
      5. Fecha a conexão
    """
    print(f"\n{'='*60}")
    print(f"ARMAZENANDO NO BANCO DE DADOS: {DB_PATH}")
    print(f"{'='*60}")

    conn = sqlite3.connect(DB_PATH)

    try:
        criar_schema(conn)
        inserir_despesas(conn, df)
        inserir_estatisticas(conn, stats)
        inserir_top_orgaos(conn, df)
        verificar_banco(conn)
        print(f"\n  Banco salvo em: {DB_PATH}")
    except Exception as e:
        print(f"  Erro ao salvar no banco: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()


def main():
    print("\n" + "="*60)
    print("  PIPELINE DE GASTOS PÚBLICOS DO GOVERNO FEDERAL")
    print("  Fonte: Portal da Transparência")
    print(f"  Período: {ANO_INICIO} a {ANO_FIM}")
    print("="*60)

  
    if API_KEY == "chave_api_aqui":
        print("\n ATENÇÃO: Configure sua chave de API!")
        print("   Cadastre em: https://portaldatransparencia.gov.br/api-de-dados/cadastrar-email")
        print("\n   Usando dados simulados para demonstração...\n")
        df, stats = usar_dados_simulados()
    else:
        
        registros = coletar_todos_dados()

        if not registros:
            print("Nenhum dado coletado. Verifique sua chave de API.")
            return

       
        df = tratar_dados(registros)

        if df.empty:
            return

        stats = calcular_estatisticas(df)

    print(f"\n{'='*60}")
    print("GERANDO GRÁFICOS")
    print(f"{'='*60}")
    if not stats.empty:
        grafico_evolucao_anual(stats)
        grafico_media_max_min(stats)
    if not df.empty:
        grafico_top_orgaos(df)

   
    salvar_banco_dados(df, stats)

    print(f"\n{'='*60}")
    print("PIPELINE CONCLUÍDO COM SUCESSO!")
    print(f"  Arquivos gerados:")
    print(f"    - {JSON_PATH}")
    print(f"    - {DB_PATH}")
    print(f"    - grafico_evolucao_anual.png")
    print(f"    - grafico_media_max_min.png")
    print(f"    - grafico_top_orgaos.png")
    print("="*60)



def usar_dados_simulados():
    """Gera dados simulados para demonstrar o pipeline."""
    import numpy as np
    np.random.seed(42)

    anos = list(range(ANO_INICIO, ANO_FIM + 1))
    orgaos = [
        "Ministério da Educação", "Ministério da Saúde",
        "Ministério da Defesa", "Ministério da Infraestrutura",
        "Ministério da Economia", "Ministério da Justiça",
        "Ministério do Meio Ambiente", "Ministério da Agricultura"
    ]

    registros = []
    for ano in anos:
        for orgao in orgaos:
            n = np.random.randint(50, 200)
            for _ in range(n):
                valor = np.random.exponential(scale=5_000_000)
                registros.append({
                    "ano": ano,
                    "mes": np.random.randint(1, 13),
                    "orgao": orgao,
                    "valor_empenhado": valor * 1.1,
                    "valor_liquidado": valor * 1.05,
                    "valor_pago": valor,
                    "favorecido": f"Favorecido {np.random.randint(1, 500)}",
                })

    df = pd.DataFrame(registros)

    # Salvar JSON simulado
    df.to_json(JSON_PATH, orient="records", force_ascii=False, indent=2)
    print(f"JSON simulado salvo: {JSON_PATH}")

    stats = df.groupby("ano")["valor_pago"].agg(
        total="sum", media="mean", maximo="max", minimo="min", contagem="count"
    ).reset_index()

    print("\nEstatísticas simuladas por ano:")
    print(stats.to_string(index=False))

    return df, stats


if __name__ == "__main__":
    main()