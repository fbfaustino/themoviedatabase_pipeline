import pandas as pd
import calendar
from config.conexao import *
from models.filme_model import *
from datetime import datetime, timedelta


def insere_filmes(data_inicial, data_final, pagina):
    dt_inicial = datetime.strptime(data_inicial, "%Y-%m-%d")
    dt_final = datetime.strptime(data_final, "%Y-%m-%d")

    while dt_inicial <= dt_final:
        ultimo_dia = calendar.monthrange(dt_inicial.year, dt_inicial.month)[1]
        dt_fim_mes = datetime(dt_inicial.year, dt_inicial.month, ultimo_dia)

        if dt_fim_mes > dt_final:
            dt_fim_mes = dt_final

        data_ini_str = dt_inicial.strftime("%Y-%m-%d")
        data_fim_str = dt_fim_mes.strftime("%Y-%m-%d")

        url = (
            "https://api.themoviedb.org/3/discover/movie?"
            f"language=pt-BR&primary_release_date.gte={data_ini_str}"
            f"&primary_release_date.lte={data_fim_str}&page=1"
        )

        data = requisicao_api(url)
        total_pages = data['total_pages']

        print(f"******* Carregando dados de {data_ini_str} *******")
        print(f"******* Até {data_fim_str} *******")
        print(f"******* Total de páginas: {total_pages} *******")

        pd.set_option('mode.chained_assignment', None)
        lista_filmes = []
        lista_generos = []

        for pagina in range(1, total_pages + 1):
            print('pagina: ' + str(pagina))
            url = (
                "https://api.themoviedb.org/3/discover/movie?"
                f"language=pt-BR&primary_release_date.gte={data_ini_str}"
                f"&primary_release_date.lte={data_fim_str}&page={pagina}"
            )
            data = requisicao_api(url)
            filmes = data['results']

            for filme in filmes:
                lista_filmes.append({
                    'id': filme['id'],
                    'title': filme['title'],
                    'overview': filme['overview'],
                    'popularity': filme['popularity'],
                    'release_date': filme['release_date'],
                    'vote_average': filme['vote_average'],
                    'vote_count': filme['vote_count'],
                    'data_dados': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                })

                for genre_id in filme['genre_ids']:
                    lista_generos.append({
                        'id_filme': filme['id'],
                        'id_genero': genre_id,
                        'data_dados': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    })

        df_filmes = pd.DataFrame(lista_filmes)
        df_generos = pd.DataFrame(lista_generos)
        if not df_filmes.empty:
            merge_filmes_snowflake(df_filmes)
        if not df_generos.empty:
            merge_generos_filmes_snowflake(df_generos)

        dt_inicial = dt_fim_mes + timedelta(days=1)