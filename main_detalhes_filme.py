from controllers.detalhes_filme_controller import insere_detalhes_filme
from datetime import datetime, timedelta


def main():
    minus_data = datetime.today() - timedelta(days=5)
    data_inicial = minus_data.strftime("%Y-%m-%d")
    data_final = datetime.today().strftime("%Y-%m-%d")

    insere_detalhes_filme(data_inicial, data_final)


if __name__ == "__main__":
    main()
