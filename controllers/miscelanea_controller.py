import pandas as pd
import glob


def join_arquivos_parquet():
    arquivos = glob.glob("C:/pipelines/imdb/files/generos2/*.parquet")
    df = pd.concat([pd.read_parquet(arquivo) for arquivo in arquivos])
    df.to_parquet("C:/pipelines/imdb/files/generos2/generos_part2.parquet", index=False)

    print("Arquivo gerado com sucesso!")


if __name__ == "__main__":
    join_arquivos_parquet()
