import duckdb
from deep_translator import GoogleTranslator

con = duckdb.connect("C:/duckdb/duckdb_imdb.db")

df = con.execute("SELECT palavra_chave FROM palavra_chave_traducao where palavra_chave_br is null and palavra_chave is not null ").fetchdf()
total_registros = len(df)
contador = 0  

print(f'Total de registros a processar: {total_registros}')

def traduzir_e_atualizar(palavra):
    global contador  
    if isinstance(palavra, str):
        try:
            traducao = GoogleTranslator(source='en', target='pt').translate(palavra)
            contador += 1
            print(f"Traduzido {contador} de {total_registros}: {palavra} -> {traducao}")

            # Atualiza o banco imediatamente
            con.execute(
                "UPDATE palavra_chave_traducao SET palavra_chave_br = ? WHERE palavra_chave = ?", 
                (traducao, palavra)
            )
        except Exception as e:
            print(f"Erro ao traduzir '{palavra}': {e}")

for index, row in df.iterrows():
    traduzir_e_atualizar(row["palavra_chave"])

print("Tradução concluída e atualizada no banco!")
