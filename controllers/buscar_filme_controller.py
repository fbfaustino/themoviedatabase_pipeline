import snowflake.connector
from models.buscar_filme_model import *
from fuzzywuzzy import fuzz


def buscar_filme(search_term):
    print('a')
    df_filmes = buscar_filmes(search_term)  # Aqui você pega seu DataFrame
    print('b')

    # Preenche valores ausentes com uma string vazia
    df_filmes = df_filmes.fillna("")
    print('c')

    # Calcula o score de similaridade para cada coluna de forma vetorizada
    score_nome = df_filmes['nome_filme'].apply(lambda x: fuzz.partial_ratio(search_term.lower(), x.lower()))
    score_palavras_chave = df_filmes['palavras_chave'].apply(lambda x: fuzz.partial_ratio(search_term.lower(), x.lower()))
    score_sinopse = df_filmes['sinopse'].apply(lambda x: fuzz.partial_ratio(search_term.lower(), x.lower()))
    score_generos = df_filmes['generos'].apply(lambda x: fuzz.partial_ratio(search_term.lower(), x.lower()))
    score_popularidade = df_filmes['popularidade'].apply(lambda x: float(x) if str(x).replace('.', '', 1).isdigit() else 0)
    print('d')
    # Calcula a média ponderada das pontuações
    score_total = (score_nome * 2) + (score_palavras_chave * 0.5) + (score_sinopse * 0.2) + (score_generos * 0.1) + score_popularidade * 10
    print('e')
    # Adiciona o score total ao DataFrame
    df_filmes['score_total'] = score_total

    # Ordena o DataFrame pela pontuação de similaridade e popularidade
    df_filmes_sorted = df_filmes.sort_values(by=['score_total', 'popularidade'], ascending=[False, False])

    print('f')
    # Retorna os 10 primeiros resultados
    return df_filmes_sorted[['id_filme', 'nome_filme', 'score_total']].head(10)
