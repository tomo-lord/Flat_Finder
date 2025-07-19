# function to update prices from calready collected data

import requests
import re
import pandas as pd
from bs4 import BeautifulSoup
from get_data import get_data
from IPython.display import clear_output


def get_price(link):
    url = link
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/"
    }
    r = requests.get(url = url, headers=headers)
    if r.status_code == 200:
        soup = BeautifulSoup(r.content, 'html5lib')
        price_element = soup.find('strong', {'data-cy': 'adPageHeaderPrice'})
        if price_element:
            price_text = price_element.text
            price_text = price_text.replace("zł", "").replace(" ", "")
            try:
                price = float(price_text)
                return price
            except ValueError:
                return None
        else: return None
    else:
        return None





def get_price_update(lista_ofert: list, df: pd.DataFrame, last_update: str) -> pd.DataFrame:
    """aktualizuje df o nowe oferty i dodaje informację o aktualnej cenie"""
    
    df['recent_price'] = df['Cena']
    df = df.rename(columns={'recent_price': last_update})
    df['Cena'] = df['link'].apply(get_price)
    nowe_unikalne_oferty = pd.Series([x for x in lista_ofert if x not in df['link'].values])
    nowe_oferty = get_data(lista_ofert=nowe_unikalne_oferty)
    df = pd.concat([df, nowe_oferty], ignore_index=True)

    return df






        
        







if __name__ == '__main__':
    price = get_price(link = 'https://www.otodom.pl/pl/oferta/klimatyczne-mieszkanie-w-sercu-warszawy-parking-ID4wuhP')
