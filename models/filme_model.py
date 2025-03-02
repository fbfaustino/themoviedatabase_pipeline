import pandas as pd
from config.conexao import get_snowflake_connection, get_schema_snowflake
from sqlalchemy import text


schema = get_schema_snowflake()


def merge_filmes_snowflake(df: pd.DataFrame):
    engine = get_snowflake_connection()

    with engine.connect() as conn:
        temp_table = "delta_filmes"
        df.to_sql(temp_table, con=conn, index=False, if_exists="replace")

        merge_sql = f"""
        MERGE INTO {schema}.filmes AS target
        USING {schema}.{temp_table} AS source
        ON target.id = source.id
        WHEN MATCHED THEN
            UPDATE SET
                title = source.title,
                overview = source.overview,
                popularity = source.popularity,
                release_date = source.release_date,
                vote_average = source.vote_average,
                vote_count = source.vote_count,
                data_dados = source.data_dados

        WHEN NOT MATCHED THEN
            INSERT (id, title, overview, popularity,
                    release_date, vote_average, vote_count, data_dados)
            VALUES (source.id, source.title, source.overview,
                    source.popularity, source.release_date,
                    source.vote_average,source.vote_count, source.data_dados)
        """
        query_merge = text(merge_sql)
        conn.execute(query_merge)

        drop_sql = f"DROP TABLE IF EXISTS {schema}.{temp_table}"
        query_drop = text(drop_sql)
        conn.execute(query_drop)

    print("Dados de filmes atualizados com sucesso!")


def merge_generos_filmes_snowflake(df: pd.DataFrame):
    engine = get_snowflake_connection()

    with engine.connect() as conn:
        temp_table = "delta_generos_filmes"
        df.to_sql(temp_table, con=conn, index=False, if_exists="replace")

        delete_sql = f"""
        DELETE FROM {schema}.generos_filmes A
        WHERE A.id_filme in 
            (SELECT DISTINCT B.id_filme FROM {schema}.{temp_table} B )
        """
        query_delete = text(delete_sql)
        conn.execute(query_delete)

        insert_sql = f"""
            INSERT INTO {schema}.generos_filmes
            SELECT id_filme,
                   id_genero,
                   data_dados
            FROM {schema}.{temp_table}
        """
        query_insert = text(insert_sql)
        conn.execute(query_insert)

        drop_sql = f"DROP TABLE IF EXISTS {schema}.{temp_table}"
        query_drop = text(drop_sql)
        conn.execute(query_drop)
        conn.close()

    print("Dados de generos dos filmes atualizados com sucesso!")