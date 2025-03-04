import pandas as pd
from config.conexao import *


def buscar_filmes(termo_busca):
    engine = get_snowflake_connection()

    with engine.connect() as conn:
        query = f"""
        SELECT id_filme, nome_filme, sinopse, palavras_chave, generos, popularidade
        FROM tmdb.gold.tab_filmes 
        where LOWER(nome_filme) like '%{termo_busca}%' or
        LOWER(sinopse) like '%{termo_busca}%' or LOWER(generos) like '%{termo_busca}%' or 
        LOWER(palavras_chave) like '%{termo_busca}%'   
        """
        query_select = text(query)
        result = conn.execute(query_select)
        data = result.fetchall()
        df = pd.DataFrame(data, columns=result.keys())

        return df