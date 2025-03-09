import requests
import pandas as pd
from config.conexao import *
from models.detalhes_filme_model import *
from datetime import datetime
from deep_translator import GoogleTranslator


URL_BASE = "https://api.themoviedb.org/3/movie/"


def insere_detalhes_filme(data_inicial, data_final):
    filmes = buscar_filmes_por_data(data_inicial, data_final)
    production_companies_list = []
    production_countries_list = []
    keywords_list = []
    detalhes_filmes_list = []
    
    count = 1
    for id in filmes['id']:
        print(f"Processando detalhes de filmes...")

        url = f"{URL_BASE}{id}?language=pt-BR"
        movie_data = requisicao_api(url)

        if movie_data:
            
            detalhes_filmes_list.append({
                'id': movie_data['id'],
                'revenue': movie_data['revenue'],
                'budget': movie_data['budget'],
                'runtime': movie_data['runtime'],
                'poster_path': movie_data['poster_path'],
                'imdb_id': movie_data['imdb_id'],
                'original_language': movie_data['original_language'],
                'original_title': movie_data['original_title'],
                'data_dados': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            })

            for company in movie_data.get("production_companies", []):
                company.pop('logo_path', None)
                company["movie_id"] = id
                production_companies_list.append({"name": company["name"], 
                                                  "id_filme": company["movie_id"],
                                                  'data_dados': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                                                  })

            for country in movie_data.get("production_countries", []):
                country["movie_id"] = id
                production_countries_list.append({"name": country["name"], 
                                                  "id_filme": country["movie_id"],
                                                  'data_dados': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                                                  })

            url_keywords = f"{URL_BASE}{id}/keywords?language=pt-BR"
            movie_keywords_data = requisicao_api(url_keywords)

            if movie_keywords_data:
                for keywords in movie_keywords_data.get("keywords", []):
                    keywords_list.append({"palavra_chave": keywords["name"],
                                           'palavra_chave_br': GoogleTranslator(source='en', target='pt').translate(keywords["name"]),
                                           "id_filme": id, 
                                           'data_dados': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                                           })

            print(f"Processando ID {id} ({count}/{len(filmes['id'])})")
            count += 1    
    
    if (len(detalhes_filmes_list) > 0 ):
        merge_detalhes_filmes_snowflake(pd.DataFrame(detalhes_filmes_list), 'temp_detalhes_filme')
    if (len(production_companies_list) > 0 ):
        salvar_produtora_snowflake(pd.DataFrame(production_companies_list), 'temp_companies')
    if (len(production_countries_list) > 0 ):
        salvar_pais_producao_snowflake(pd.DataFrame(production_countries_list), 'temp_countries')
    if(len(keywords_list) > 0):
        salvar_palavras_chave_snowflake(pd.DataFrame(keywords_list), 'temp_keywords')

    print("Processamento concluído com sucesso!")
