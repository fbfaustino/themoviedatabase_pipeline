from controllers.filme_controller import insere_filmes
from datetime import datetime, timedelta


def main():
    pagina = 1
    minus_data = datetime.today() - timedelta(days=2)
    data_inicial = minus_data.strftime("%Y-%m-%d")
    data_final = datetime.today().strftime("%Y-%m-%d")

    insere_filmes(data_inicial, data_final, pagina)


if __name__ == "__main__":
    main()
