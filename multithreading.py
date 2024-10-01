import requests
import time
import csv
import random
import concurrent.futures
from bs4 import BeautifulSoup

# global headers to be used for requests
# Cabeçalho global que será usado para as requisições HTTP, identificando o navegador
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Safari/537.36 Edge/12.246'}

# Definindo o número máximo de threads simultâneas
MAX_THREADS = 10

# Função para extrair os detalhes de um filme a partir de seu link
def extract_movie_details(movie_link):
    # Aguarda um intervalo aleatório entre 0 e 0,2 segundos para evitar sobrecarregar o servidor
    time.sleep(random.uniform(0, 0.2))
    
    # Faz uma requisição HTTP para obter o conteúdo da página do filme
    response = requests.get(movie_link, headers=headers)
    
    # Usa o BeautifulSoup para analisar o HTML da página do filme
    movie_soup = BeautifulSoup(response.content, 'html.parser')

    # Verifica se o BeautifulSoup conseguiu parsear corretamente
    if movie_soup is not None:
        title = None
        date = None
        
        # Encontrando a seção específica que contém as informações do filme
        page_section = movie_soup.find('section', attrs={'class': 'ipc-page-section'})
        
        # Se a seção foi encontrada, prossiga com a extração dos dados
        if page_section is not None:
            # Busca todas as divs diretamente dentro da seção (não procura recursivamente)
            divs = page_section.find_all('div', recursive=False)
            
            # Se houver mais de uma div, a segunda contém as informações desejadas
            if len(divs) > 1:
                target_div = divs[1]
                
                # Busca a tag h1 dentro da div para obter o título do filme
                title_tag = target_div.find('h1')
                if title_tag:
                    # Pega o texto do título
                    title = title_tag.find('span').get_text()
                
                # Busca o link que aponta para a página de informações de lançamento do filme
                date_tag = target_div.find('a', href=lambda href: href and 'releaseinfo' in href)
                if date_tag:
                    # Pega o texto da data de lançamento
                    date = date_tag.get_text().strip()
                
                # Busca a classificação do filme (nota)
                rating_tag = movie_soup.find('div', attrs={'data-testid': 'hero-rating-bar__aggregate-rating__score'})
                rating = rating_tag.get_text() if rating_tag else None
                
                # Busca a sinopse do filme
                plot_tag = movie_soup.find('span', attrs={'data-testid': 'plot-xs_to_m'})
                plot_text = plot_tag.get_text().strip() if plot_tag else None
                
                # Abre o arquivo CSV para gravar os dados do filme
                with open('movies.csv', mode='a', newline='', encoding='utf-8') as file:
                    movie_writer = csv.writer(file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                    
                    # Se todos os dados forem encontrados (título, data, classificação e sinopse), escreve no CSV
                    if all([title, date, rating, plot_text]):
                        print(title, date, rating, plot_text)  # Exibe os detalhes do filme no console
                        movie_writer.writerow([title, date, rating, plot_text])  # Escreve os detalhes no arquivo

# Função para extrair os links dos filmes da página principal
def extract_movies(soup):
    # Busca a tabela de filmes populares na página do IMDB
    movies_table = soup.find('div', attrs={'data-testid': 'chart-layout-main-column'}).find('ul')
    
    # Busca todas as linhas (li) da lista de filmes
    movies_table_rows = movies_table.find_all('li')
    
    # Cria uma lista com os links dos filmes extraídos da lista
    movie_links = ['https://imdb.com' + movie.find('a')['href'] for movie in movies_table_rows]

    # Define a quantidade de threads a ser usada, que será o mínimo entre MAX_THREADS e o número de links de filmes
    threads = min(MAX_THREADS, len(movie_links))
    
    # Cria um ThreadPoolExecutor para executar a extração de detalhes dos filmes em paralelo
    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
        # O map aplica a função extract_movie_details em cada link da lista de forma paralela
        executor.map(extract_movie_details, movie_links)

# Função principal
def main():
    # Marca o tempo inicial para medir a performance
    start_time = time.time()

    # URL da página de filmes mais populares no IMDB
    popular_movies_url = 'https://www.imdb.com/chart/moviemeter/?ref_=nv_mv_mpm'
    
    # Faz uma requisição HTTP para obter o conteúdo da página de filmes mais populares
    response = requests.get(popular_movies_url, headers=headers)
    
    # Usa o BeautifulSoup para analisar o HTML da página de filmes populares
    soup = BeautifulSoup(response.content, 'html.parser')

    # Chama a função principal que extrai os filmes da página do IMDB
    extract_movies(soup)

    # Marca o tempo final e calcula o tempo total de execução
    end_time = time.time()
    print('Total time taken: ', end_time - start_time)

# Inicia o programa quando o script é executado
if __name__ == '__main__':
    main()
