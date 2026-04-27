
import requests
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import seaborn as sns
import mysql.connector
import json
import os
import logging
import schedule
import time
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("pipeline.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

# ── Configurações ─────────────────────────────────────────────────────────────
API_KEY  = os.getenv("API_KEY")
BASE_URL = "https://api.portaldatransparencia.gov.br/api-de-dados"
JSON_PATH = "gastos_publicos.json"

HEADERS = {
    "chave-api-dados": API_KEY,
    "Accept": "application/json"
}

ANO_INICIO = datetime.now().year - 10
ANO_FIM    = datetime.now().year - 1  # Ano anterior (dados completos)


# ── Conexão MySQL ─────────────────────────────────────────────────────────────
def get_mysql_connection() -> mysql.connector.MySQLConnection:
    db_name = os.getenv("MYSQL_DATABASE", "gastos_publicos")

    # Conecta sem banco para garantir que ele existe
    tmp = mysql.connector.connect(
        host     = os.getenv("MYSQL_HOST",     "localhost"),
        user     = os.getenv("MYSQL_USER",     "root"),
        password = os.getenv("MYSQL_PASSWORD", ""),
    )
    cursor = tmp.cursor()
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{db_name}` CHARACTER SET utf8mb4")
    cursor.close()
    tmp.disconnect()

    conn = mysql.connector.connect(
        host     = os.getenv("MYSQL_HOST",     "localhost"),
        user     = os.getenv("MYSQL_USER",     "root"),
        password = os.getenv("MYSQL_PASSWORD", ""),
        database = db_name,
    )
    logger.info("Conexão MySQL estabelecida (banco: %s).", db_name)
    return conn


# ── Coleta ────────────────────────────────────────────────────────────────────
def coletar_despesas_por_ano(ano: int, paginas: int = 3) -> list:
    todos_registros = []
    mes_inicio = f"01/{ano}"
    mes_fim    = f"12/{ano}"

    for pagina in range(1, paginas + 1):
        params = {
            "mesAnoInicio": mes_inicio,
            "mesAnoFim":    mes_fim,
            "pagina":       pagina,
        }
        try:
            response = requests.get(
                f"{BASE_URL}/despesas/recursos-recebidos",
                headers=HEADERS,
                params=params,
                timeout=30,
            )
            if response.status_code == 200:
                dados = response.json()
                if not dados:
                    break
                todos_registros.extend(dados)
                logger.info("Ano %d | Página %d: %d registros coletados", ano, pagina, len(dados))
            else:
                logger.warning("Erro %d ao coletar ano %d, página %d", response.status_code, ano, pagina)
                break
        except requests.exceptions.RequestException as e:
            logger.error("Erro de conexão: %s", e)
            break

    return todos_registros


def coletar_todos_dados() -> list:
    logger.info("=" * 60)
    logger.info("COLETANDO DADOS: %d a %d", ANO_INICIO, ANO_FIM)
    logger.info("=" * 60)

    todos = []
    for ano in range(ANO_INICIO, ANO_FIM + 1):
        logger.info("Processando ano %d...", ano)
        registros = coletar_despesas_por_ano(ano, paginas=2)
        todos.extend(registros)
        logger.info("Total acumulado: %d registros", len(todos))

    logger.info("Coleta concluída! Total: %d registros", len(todos))
    return todos


# ── Tratamento ────────────────────────────────────────────────────────────────
def tratar_dados(registros: list) -> pd.DataFrame:
    logger.info("=" * 60)
    logger.info("TRATANDO DADOS")
    logger.info("=" * 60)

    df = pd.DataFrame(registros)

    if df.empty:
        logger.warning("Nenhum dado para tratar.")
        return df

    logger.info("Colunas disponíveis: %s", list(df.columns))

    colunas_map = {
        "codigoOrgao":        "cod_orgao",
        "nomeOrgao":          "orgao",
        "valorEmpenhado":     "valor_empenhado",
        "valorLiquidado":     "valor_liquidado",
        "valorPago":          "valor_pago",
        "mesAno":             "mes_ano",
        "nomeFavorecido":     "favorecido",
        "cpfCnpjFavorecido":  "cpf_cnpj",
    }
    df = df.rename(columns={k: v for k, v in colunas_map.items() if k in df.columns})

    for col in ["valor_empenhado", "valor_liquidado", "valor_pago"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    if "mes_ano" in df.columns:
        df["mes_ano"] = df["mes_ano"].astype(str)
        df["ano"] = df["mes_ano"].str[-4:].astype(int, errors="ignore")
        df["mes"] = df["mes_ano"].str[:2].astype(int, errors="ignore")

    df = df.drop_duplicates()

    logger.info("Registros após tratamento: %d", len(df))
    if "ano" in df.columns:
        logger.info("Período: %d a %d", df["ano"].min(), df["ano"].max())

    salvar_json(df)
    return df


def salvar_json(df: pd.DataFrame):
    df.to_json(JSON_PATH, orient="records", force_ascii=False, indent=2)
    logger.info("Dados salvos em JSON: %s", JSON_PATH)


# ── Estatísticas ──────────────────────────────────────────────────────────────
def calcular_estatisticas(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("=" * 60)
    logger.info("CALCULANDO ESTATÍSTICAS")
    logger.info("=" * 60)

    if "ano" not in df.columns or "valor_pago" not in df.columns:
        logger.warning("Colunas necessárias não encontradas.")
        return pd.DataFrame()

    stats = df.groupby("ano")["valor_pago"].agg(
        total="sum", media="mean", maximo="max", minimo="min", contagem="count"
    ).reset_index()

    logger.info("Estatísticas por ano (valor pago em R$):\n%s", stats.to_string(index=False))
    return stats


# ── Gráficos ──────────────────────────────────────────────────────────────────
def formatar_reais(valor, _):
    if abs(valor) >= 1e9:
        return f"R$ {valor/1e9:.1f}B"
    elif abs(valor) >= 1e6:
        return f"R$ {valor/1e6:.1f}M"
    return f"R$ {valor:,.0f}"


def grafico_evolucao_anual(stats: pd.DataFrame):
    """Gráfico de barras com a evolução total dos gastos por ano."""
    fig, ax = plt.subplots(figsize=(12, 6))
    bars = ax.bar(stats["ano"], stats["total"], color="#1a6bb0", edgecolor="white", linewidth=0.5)

    for bar, val in zip(bars, stats["total"]):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() * 1.01,
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
    logger.info("Gráfico salvo: grafico_evolucao_anual.png")


def grafico_media_max_min(stats: pd.DataFrame):
    """Gráfico de linhas com média, máximo e mínimo por ano, com área sombreada entre min e max."""
    fig, ax = plt.subplots(figsize=(12, 6))
    x = stats["ano"]

    # Três séries: média (azul), máximo (vermelho), mínimo (verde)
    ax.plot(x, stats["media"],  marker="o", label="Média",   color="#2196F3", linewidth=2)
    ax.plot(x, stats["maximo"], marker="^", label="Máximo",  color="#F44336", linewidth=2)
    ax.plot(x, stats["minimo"], marker="v", label="Mínimo",  color="#4CAF50", linewidth=2)

    # Área sombreada entre mínimo e máximo para destacar a amplitude
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
    logger.info("Gráfico salvo: grafico_media_max_min.png")


def grafico_top_orgaos(df: pd.DataFrame):
    """Gráfico horizontal de barras com os 10 órgãos de maior gasto total."""
    if "orgao" not in df.columns or "valor_pago" not in df.columns:
        logger.warning("Colunas necessárias não encontradas. Disponíveis: %s", list(df.columns))
        return

    top = df.groupby("orgao")["valor_pago"].sum().nlargest(10).reset_index()

    fig, ax = plt.subplots(figsize=(12, 7))
    # Paleta de azuis degradê para diferenciar o ranking visualmente
    colors = sns.color_palette("Blues_d", len(top))
    bars = ax.barh(top["orgao"], top["valor_pago"], color=colors)

    for bar, val in zip(bars, top["valor_pago"]):
        ax.text(val * 1.01, bar.get_y() + bar.get_height() / 2,
                formatar_reais(val, None), va="center", fontsize=8)

    ax.set_title("Top 10 Órgãos com Maiores Gastos\n(últimos 10 anos)", fontsize=14, fontweight="bold")
    ax.set_xlabel("Total Pago (R$)", fontsize=11)
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(formatar_reais))
    ax.invert_yaxis()
    ax.grid(axis="x", linestyle="--", alpha=0.4)
    plt.tight_layout()
    plt.savefig("grafico_top_orgaos.png", dpi=150)
    plt.show()
    logger.info("Gráfico salvo: grafico_top_orgaos.png")


# ── Banco de Dados (MySQL) ────────────────────────────────────────────────────
def criar_schema(conn: mysql.connector.MySQLConnection):
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS despesas (
            id              INT AUTO_INCREMENT PRIMARY KEY,
            ano             INT,
            mes             INT,
            cod_orgao       VARCHAR(50),
            orgao           VARCHAR(255),
            favorecido      VARCHAR(255),
            cpf_cnpj        VARCHAR(20),
            valor_empenhado DOUBLE DEFAULT 0.0,
            valor_liquidado DOUBLE DEFAULT 0.0,
            valor_pago      DOUBLE DEFAULT 0.0,
            inserido_em     DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS estatisticas_anuais (
            id       INT AUTO_INCREMENT PRIMARY KEY,
            ano      INT UNIQUE,
            total    DOUBLE,
            media    DOUBLE,
            maximo   DOUBLE,
            minimo   DOUBLE,
            contagem INT
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS top_orgaos (
            id         INT AUTO_INCREMENT PRIMARY KEY,
            orgao      VARCHAR(255) UNIQUE,
            total_pago DOUBLE,
            ranking    INT
        )
    """)

    # Índices para consultas rápidas por ano e por órgão
    for ddl in [
        "CREATE INDEX IF NOT EXISTS idx_despesas_ano   ON despesas (ano)",
        "CREATE INDEX IF NOT EXISTS idx_despesas_orgao ON despesas (orgao(100))",
    ]:
        try:
            cursor.execute(ddl)
        except mysql.connector.Error:
            pass  # índice já existe

    conn.commit()
    cursor.close()
    logger.info("Schema criado (tabelas + índices)")


def inserir_despesas(conn: mysql.connector.MySQLConnection, df: pd.DataFrame):
    colunas_db = ["ano", "mes", "cod_orgao", "orgao", "favorecido",
                  "cpf_cnpj", "valor_empenhado", "valor_liquidado", "valor_pago"]
    colunas_presentes = [c for c in colunas_db if c in df.columns]
    df_inserir = df[colunas_presentes].copy()

    cursor = conn.cursor()
    cursor.execute("DELETE FROM despesas")  # replace equivalente

    placeholders = ", ".join(["%s"] * len(colunas_presentes))
    sql = f"INSERT INTO despesas ({', '.join(colunas_presentes)}) VALUES ({placeholders})"
    rows = [tuple(None if pd.isna(v) else v for v in row) for row in df_inserir.itertuples(index=False)]

    cursor.executemany(sql, rows)
    conn.commit()
    cursor.close()
    logger.info("Tabela 'despesas': %d registros inseridos", len(rows))


def inserir_estatisticas(conn: mysql.connector.MySQLConnection, stats: pd.DataFrame):
    if stats.empty:
        logger.warning("Nenhuma estatística para inserir.")
        return

    cursor = conn.cursor()
    cursor.execute("DELETE FROM estatisticas_anuais")

    sql = """
        INSERT INTO estatisticas_anuais (ano, total, media, maximo, minimo, contagem)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    rows = [tuple(row) for row in stats[["ano", "total", "media", "maximo", "minimo", "contagem"]].itertuples(index=False)]
    cursor.executemany(sql, rows)
    conn.commit()
    cursor.close()
    logger.info("Tabela 'estatisticas_anuais': %d registros inseridos", len(rows))


def inserir_top_orgaos(conn: mysql.connector.MySQLConnection, df: pd.DataFrame):
    if "orgao" not in df.columns or "valor_pago" not in df.columns:
        logger.warning("Colunas para ranking não encontradas. Pulando top_orgaos.")
        return

    top = (
        df.groupby("orgao")["valor_pago"]
        .sum()
        .nlargest(10)
        .reset_index()
        .rename(columns={"valor_pago": "total_pago"})
    )
    top["ranking"] = range(1, len(top) + 1)

    cursor = conn.cursor()
    cursor.execute("DELETE FROM top_orgaos")

    sql = "INSERT INTO top_orgaos (orgao, total_pago, ranking) VALUES (%s, %s, %s)"
    rows = [(row.orgao, row.total_pago, row.ranking) for row in top.itertuples(index=False)]
    cursor.executemany(sql, rows)
    conn.commit()
    cursor.close()
    logger.info("Tabela 'top_orgaos': %d registros inseridos", len(rows))


def verificar_banco(conn: mysql.connector.MySQLConnection):
    cursor = conn.cursor()

    logger.info("--- Verificação do banco ---")
    cursor.execute("SHOW TABLES")
    tabelas = [row[0] for row in cursor.fetchall()]
    logger.info("Tabelas: %s", tabelas)

    for tabela in tabelas:
        cursor.execute(f"SELECT COUNT(*) FROM {tabela}")
        total = cursor.fetchone()[0]
        logger.info("%s: %d registros", tabela, total)

    if "despesas" in tabelas:
        cursor.execute("SELECT DISTINCT ano FROM despesas ORDER BY ano")
        anos = [row[0] for row in cursor.fetchall()]
        logger.info("Anos em 'despesas': %s", anos)

    if "top_orgaos" in tabelas:
        cursor.execute("SELECT ranking, orgao, total_pago FROM top_orgaos ORDER BY ranking LIMIT 3")
        for row in cursor.fetchall():
            logger.info("  #%d %s: R$ %,.0f", row[0], row[1], row[2])

    cursor.close()


def salvar_banco_dados(df: pd.DataFrame, stats: pd.DataFrame):
    logger.info("=" * 60)
    logger.info("ARMAZENANDO NO BANCO DE DADOS (MySQL)")
    logger.info("=" * 60)

    conn = get_mysql_connection()
    try:
        criar_schema(conn)
        inserir_despesas(conn, df)
        inserir_estatisticas(conn, stats)
        inserir_top_orgaos(conn, df)
        verificar_banco(conn)
        logger.info("Dados salvos no MySQL com sucesso.")
    except Exception as e:
        logger.error("Erro ao salvar no banco: %s", e)
        conn.rollback()
        raise
    finally:
        conn.disconnect()


# ── Pipeline principal ────────────────────────────────────────────────────────
def run_pipeline():
    logger.info("\n" + "=" * 60)
    logger.info("  PIPELINE DE GASTOS PÚBLICOS DO GOVERNO FEDERAL")
    logger.info("  Fonte: Portal da Transparência")
    logger.info("  Período: %d a %d", ANO_INICIO, ANO_FIM)
    logger.info("=" * 60)

    if API_KEY == "palavra":
        df, stats = usar_dados_simulados()
    else:
        registros = coletar_todos_dados()

        if not registros:
            logger.error("Nenhum dado coletado. Verifique sua chave de API.")
            return

        df = tratar_dados(registros)
        if df.empty:
            return

        stats = calcular_estatisticas(df)

    logger.info("=" * 60)
    logger.info("GERANDO GRÁFICOS")
    logger.info("=" * 60)

    if not stats.empty:
        grafico_evolucao_anual(stats)
        grafico_media_max_min(stats)
    if not df.empty:
        grafico_top_orgaos(df)

    salvar_banco_dados(df, stats)

    logger.info("=" * 60)
    logger.info("PIPELINE CONCLUÍDO COM SUCESSO!")
    logger.info("  Arquivos gerados:")
    logger.info("    - %s", JSON_PATH)
    logger.info("    - grafico_evolucao_anual.png")
    logger.info("    - grafico_media_max_min.png")
    logger.info("    - grafico_top_orgaos.png")
    logger.info("=" * 60)


# ── Dados simulados ───────────────────────────────────────────────────────────
def usar_dados_simulados():
    import numpy as np
    np.random.seed(42)

    anos   = list(range(ANO_INICIO, ANO_FIM + 1))
    orgaos = [
        "Ministério da Educação", "Ministério da Saúde",
        "Ministério da Defesa",   "Ministério da Infraestrutura",
        "Ministério da Economia", "Ministério da Justiça",
        "Ministério do Meio Ambiente", "Ministério da Agricultura",
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
    df.to_json(JSON_PATH, orient="records", force_ascii=False, indent=2)
    logger.info("JSON simulado salvo: %s", JSON_PATH)

    stats = df.groupby("ano")["valor_pago"].agg(
        total="sum", media="mean", maximo="max", minimo="min", contagem="count"
    ).reset_index()

    logger.info("Estatísticas simuladas por ano:\n%s", stats.to_string(index=False))
    return df, stats


# ── Agendamento ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # Executa uma vez imediatamente e depois agenda para rodar todo dia à meia-noite
    run_pipeline()

    schedule.every().day.at("00:00").do(run_pipeline)
    logger.info("Agendamento ativo: pipeline rodará diariamente à meia-noite. Ctrl+C para encerrar.")

    while True:
        schedule.run_pending()
        time.sleep(60)
