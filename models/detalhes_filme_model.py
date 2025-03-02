import pandas as pd
from config.conexao import *
from datetime import datetime

schema = get_schema_snowflake()

def buscar_filmes_por_data(start_date, end_date):
    engine = get_snowflake_connection()

    with engine.connect() as conn:
        query = f"""
        SELECT id
        FROM filmes
        WHERE release_date BETWEEN '{start_date}' AND '{end_date}'
        """
        query_select = text(query)
        result = conn.execute(query_select)
        data = result.fetchall()
        df = pd.DataFrame(data, columns=result.keys())

        return df


def merge_detalhes_filmes_snowflake(df: pd.DataFrame, temp_table):
    engine = get_snowflake_connection()

    with engine.connect() as conn:
        df.to_sql(temp_table, con=conn, index=False, if_exists="replace")
        merge_sql = f"""
        MERGE INTO {schema}.detalhes_filmes AS target
        USING {schema}.{temp_table} AS source
        ON target.id = source.id
        WHEN MATCHED THEN
            UPDATE SET
                revenue = source.revenue,
                budget = source.budget,
                runtime = source.runtime,
                poster_path = source.poster_path,
                imdb_id = source.imdb_id,
                original_language = source.original_language,
                original_title = source.original_title,
                data_dados = source.data_dados

        WHEN NOT MATCHED THEN
            INSERT (id, revenue, budget, runtime, poster_path, 
                    imdb_id, original_language, original_title, data_dados)
            VALUES (source.id, source.revenue, source.budget, source.runtime, source.poster_path, 
                    source.imdb_id, source.original_language, source.original_title, source.data_dados)
        """
        query_merge = text(merge_sql)
        conn.execute(query_merge)
        drop_sql = f"DROP TABLE IF EXISTS {schema}.{temp_table}"
        query_drop = text(drop_sql)
        conn.execute(query_drop)


def salvar_produtora_snowflake(df: pd.DataFrame, temp_table):
    engine = get_snowflake_connection()

    with engine.connect() as conn:
        df.to_sql(temp_table, con=conn, index=False, if_exists="replace")

        delete_sql = f"""
        DELETE FROM {schema}.produtora A
        WHERE A.id_filme in 
            (SELECT DISTINCT B.id_filme FROM {schema}.{temp_table} B )
        """
        query_delete = text(delete_sql)
        conn.execute(query_delete)

        insert_sql = f"""
            INSERT INTO {schema}.produtora
            SELECT id_filme, name, data_dados
            FROM {schema}.{temp_table}
        """
        query_insert = text(insert_sql)
        conn.execute(query_insert)

        drop_sql = f"DROP TABLE IF EXISTS {schema}.{temp_table}"
        query_drop = text(drop_sql)
        conn.execute(query_drop)
        conn.close()


def salvar_pais_producao_snowflake(df: pd.DataFrame, temp_table):
    engine = get_snowflake_connection()

    with engine.connect() as conn:
        df.to_sql(temp_table, con=conn, index=False, if_exists="replace")

        delete_sql = f"""
        DELETE FROM {schema}.pais_produtor A
        WHERE A.id_filme in 
            (SELECT DISTINCT B.id_filme FROM {schema}.{temp_table} B )
        """
        query_delete = text(delete_sql)
        conn.execute(query_delete)

        insert_sql = f"""
            INSERT INTO {schema}.pais_produtor
            SELECT id_filme, name, data_dados
            FROM {schema}.{temp_table}
        """
        query_insert = text(insert_sql)
        conn.execute(query_insert)

        drop_sql = f"DROP TABLE IF EXISTS {schema}.{temp_table}"
        query_drop = text(drop_sql)
        conn.execute(query_drop)
        conn.close()


def salvar_palavras_chave_snowflake(df: pd.DataFrame, temp_table):
    engine = get_snowflake_connection()

    with engine.connect() as conn:
        df.to_sql(temp_table, con=conn, index=False, if_exists="replace")

        delete_sql = f"""
        DELETE FROM {schema}.palavras_chave A
        WHERE A.id_filme in 
            (SELECT DISTINCT B.id_filme FROM {schema}.{temp_table} B )
        """
        query_delete = text(delete_sql)
        conn.execute(query_delete)

        insert_sql = f"""
            INSERT INTO {schema}.palavras_chave
            SELECT id_filme, palavra_chave, palavra_chave_br, data_dados
            FROM {schema}.{temp_table}
        """
        query_insert = text(insert_sql)
        conn.execute(query_insert)

        drop_sql = f"DROP TABLE IF EXISTS {schema}.{temp_table}"
        query_drop = text(drop_sql)
        conn.execute(query_drop)
        conn.close()
