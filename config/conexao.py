import requests
import os
from sqlalchemy import create_engine, text
from snowflake.sqlalchemy import URL
from dotenv import load_dotenv

load_dotenv()

DB_PATH = 'C:/duckdb/duckdb_imdb.db'
PARQUET_PATH = 'C:/pipelines/filmesimdb/files/'

ACCOUNT = os.getenv("ACCOUNT")
USER = os.getenv("USER")
PASSWORD = os.getenv("PASSWORD")
DATABASE = os.getenv("DATABASE")
SCHEMA = os.getenv("SCHEMA")
WAREHOUSE = os.getenv("WAREHOUSE")
ROLE = os.getenv("ROLE")


def get_schema_snowflake():
    return SCHEMA


def get_snowflake_connection():
    return create_engine(URL(
        account=ACCOUNT,
        user=USER,
        password=PASSWORD,
        database=DATABASE,
        schema=SCHEMA,
        warehouse=WAREHOUSE,
        role=ROLE
    ))


def header_api():
    API_TOKEN = os.getenv("API_TOKEN")

    if not API_TOKEN:
        raise ValueError("API_TOKEN não encontrado no arquivo .env.")

    return {
        "accept": "application/json",
        "Authorization": f"Bearer {API_TOKEN}"
    }


def requisicao_api(url):
    response = requests.get(url, headers=header_api())
    if response.status_code != 200:
        print(f"Erro ao acessar API: {response.status_code} - {response.text}")
        return None
    return response.json()

def gera_arquivo_parquet(df, nome_arquivo):
    df.to_parquet(PARQUET_PATH + str(nome_arquivo), engine="pyarrow", index=False)
