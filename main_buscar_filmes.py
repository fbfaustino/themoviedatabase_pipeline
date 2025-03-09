from controllers.buscar_filme_controller import buscar_filme


def main():
    search_term = 'estou aqui' #input("Digite a palavra que deseja buscar: ")
    results = buscar_filme(search_term.lower())
    
    print(f"Filmes encontrados para '{search_term}':\n")
    print(results)

if __name__ == "__main__":
    main()
