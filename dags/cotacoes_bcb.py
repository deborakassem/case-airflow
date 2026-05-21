from datetime import datetime
from airflow.sdk import DAG

from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

import pandas as pd
import requests
import logging

from io import StringIO

with DAG(
    dag_id='fin_cotacoes_bcb',
    schedule='@daily',
    start_date=datetime(2026, 1, 1),
    catchup=False,  # Se True, a DAG terá uma execução para cada data entre start_date e hoje. Se False, só roda a partir de hoje.
    default_args={
        'owner': 'airflow',
        'retries': 1,
    },
    tags=["bcb"]
) as dag:
    pass


# -----------------------------------------
# ---------------- EXTRACT ----------------
# -----------------------------------------

def extract_cotacoes_bcb(**kwargs):
    
    ds_nodash = kwargs['ds_nodash']  # Data de execução no formato YYYYMMDD

    base_url = "https://www4.bcb.gov.br/Download/fechamento/"
    full_url = f"{base_url}{ds_nodash}.csv"
    logging.info(f"Baixando dados de: {full_url}")

    try:
        response = requests.get(full_url)
        if response.status_code == 200: # 200 significa que a requisição foi bem-sucedida
            csv_data = response.content.decode('utf-8')
            return csv_data
        else:
            logging.error(f"Erro ao baixar dados: {response.status_code} - {response.text}")
            raise Exception(f"Erro ao baixar dados: {response.status_code}")
    except Exception as e:
        logging.error(f"Exceção ao baixar dados: {e}")
        raise

extract_task = PythonOperator(
    task_id='extract_cotacoes_bcb',
    python_callable=extract_cotacoes_bcb,
    dag=dag
)



# -------------------------------------------
# ---------------- TRANSFORM ----------------
# -------------------------------------------

def transform_cotacoes_bcb(**kwargs):

    # XCom serve para compartilhar dados entre tarefas.
    # Aqui, estamos puxando os dados CSV que foram baixados na tarefa de extração.
    
    # Não é recomendado usar XCom para produção, da mesma forma que no Vertex AI
    # um componente não recebe diretamente o conetúdo do componente anterior
    # sem que ele tenha sido gravado e exportado de um outro lugar de armazenamento (ex: BigQuery).
    cotacoes_csv = kwargs['ti'].xcom_pull(task_ids='extract_cotacoes_bcb')
    
    # Transforma o CSV em uma string
    csvStringIO = StringIO(cotacoes_csv)

    column_names = [
        "DT_FECHAMENTO",
        "COD_MOEDA",
        "TIPO_MOEDA",
        "DESC_MOEDA",
        "TAXA_COMPRA",
        "TAXA_VENDA",
        "PARIDADE_COMPRA",
        "PARIDADE_VENDA"
    ]

    data_types = {
        "DT_FECHAMENTO": str,
        "COD_MOEDA": str,
        "TIPO_MOEDA": str,
        "DESC_MOEDA": str,
        "TAXA_COMPRA": float,
        "TAXA_VENDA": float,
        "PARIDADE_COMPRA": float,
        "PARIDADE_VENDA": float
    }

    parse_dates = ["DT_FECHAMENTO"]

    # Cria DataFrame
    df = pd.read_csv(
        csvStringIO,
        sep=";",
        decimal=",",
        thousands=".",
        encoding="utf-8",
        harder=None,
        names=column_names,
        dtype=data_types,
        parse_dates=parse_dates
    )

    df["dt_processamento"] = datetime.now()
    return df

transform_task = PythonOperator(
    task_id='transform_cotacoes_bcb',
    python_callable=transform_cotacoes_bcb,
    dag=dag
)



# ------------------------------------------
# -------------- CREATE TABLE --------------
# ------------------------------------------

# Carregar a tabela no Postgres

create_table_ddl = """"
    CREATE TABLE IF NOT EXISTS cotacoes (
        dt_processamento TIMESTAMP
        dt_fechamento DATE,
        cod_moeda TEXT,
        tipo_moeda TEXT,
        desc_moeda TEXT,
        taxa_compra REAL,
        taxa_venda REAL,
        paridade_compra REAL,
        paridade_venda REAL,
        CONSTRAINT table_pk
        PRIMARY KEY (dt_fechamento, cod_moeda
    )
"""

create_table_postgres = SQLExecuteQueryOperator(
    task_id="create_table_postgres_cotacoes_bcb",
    conn_id="postgres_astro",
    sql=create_table_ddl,
    dag=dag
)



# ------------------------------------------
# ------------------ LOAD ------------------
# ------------------------------------------

def load_cotacoes_bcb(**kwargs):
    cotacoes_df = kwargs['ti'].xcom_pull(task_ids='transform')
    table_name = "cotacoes"

    postgres_hook = PostgresHook(
        postgres_conn_id="postgres_astro",
        schema="astro"
    )

    rows = list(cotacoes_df.itertuples(index=False))

    postgres_hook.insert_rows(
        table_name,
        rows,
        replace=True,
        replace_index=["DT_FECHAMENTO", "COD_MOEDA"],
        target_fields=[
            "DT_PROCESSAMENTO",
            "DT_FECHAMENTO",
            "COD_MOEDA",
            "TIPO_MOEDA",
            "DESC_MOEDA",
            "TAXA_COMPRA",
            "TAXA_VENDA",
            "PARIDADE_COMPRA",
            "PARIDADE_VENDA"
        ]
    )

load_task = PythonOperator(
    task_id='load_cotacoes_bcb',
    python_callable=load_cotacoes_bcb,
    dag=dag
)



# -------------------------------------------
# ------------ ORGANIZANDO A DAG ------------
# -------------------------------------------

extract_task >> transform_task >> create_table_postgres >> load_task