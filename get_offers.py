from urllib import request
from bs4 import BeautifulSoup
import requests, re, os


def get_offers(pages = 50) -> list:
    """
    function to get you offers from x amount of pages of results
    """
    
    # Tworzenie listy linków do ofert dla ofert z pierwszych 50 stron


    lista_ofert = []

    for page in range(pages):
        url = f"https://www.otodom.pl/pl/wyniki/sprzedaz/mieszkanie/mazowieckie/warszawa/warszawa/warszawa?viewType=listing&page={page+1}"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/"
        }
        r = requests.get(url, headers=headers)

        if r.status_code == 200:
            soup = BeautifulSoup(r.content, 'html5lib')
            html_string = str(soup)
            #print("Success")
        else:
            print(f"Failed to retrieve the webpage. Status code: {r.status_code}")
            break
            
        content = html_string

        # Parse the HTML content using BeautifulSoup
        soup = BeautifulSoup(content, 'html.parser')

        # Find all 'a' tags
        links = soup.find_all('a', href=True)

        # Extract the URLs
        urls = [link['href'] for link in links]

        # Filter the URLs that start with "/pl/oferta/"
        filtered_urls = [url for url in urls if url.startswith('/pl/oferta/')]
        unique_urls = list(set(filtered_urls))
        for element in unique_urls:
            lista_ofert.append(element)

    print(f"Liczba ofert: {len(lista_ofert)}")
    return lista_ofert

if __name__ == '__main__':
    lista_ofert = get_offers(pages=1)