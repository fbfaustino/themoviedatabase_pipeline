import snowflake.connector
from models.buscar_filme_model import *
from fuzzywuzzy import fuzz


def buscar_filme(search_term):
    df_filmes = buscar_filmes(search_term)

    df_filmes = df_filmes.fillna("")

    score_nome = df_filmes['nome_filme'].apply(lambda x: fuzz.partial_ratio(search_term.lower(), x.lower()))
    score_palavras_chave = df_filmes['palavras_chave'].apply(lambda x: fuzz.partial_ratio(search_term.lower(), x.lower()))
    score_sinopse = df_filmes['sinopse'].apply(lambda x: fuzz.partial_ratio(search_term.lower(), x.lower()))
    score_generos = df_filmes['generos'].apply(lambda x: fuzz.partial_ratio(search_term.lower(), x.lower()))
    score_popularidade = df_filmes['popularidade'].apply(lambda x: float(x) if str(x).replace('.', '', 1).isdigit() else 0)
    score_total = (score_nome * 2) + (score_palavras_chave * 0.5) + (score_sinopse * 0.2) + (score_generos * 0.1) + score_popularidade * 10

    df_filmes['score_total'] = score_total

    df_filmes_sorted = df_filmes.sort_values(by=['score_total', 'popularidade'], ascending=[False, False])

    return df_filmes_sorted[['id_filme', 'nome_filme', 'score_total']].head(10)
