import requests
import re
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from IPython.display import clear_output
from tqdm import tqdm




def get_data(lista_ofert: list) -> pd.DataFrame:
    """Pobiera dane z listy ofert i zwraca DataFrame."""

    #inicjalizacja słownika list
    data = {
        "Tytuł oferty": [],
        "link": [],
        "Cena": [],
        "Powierzchnia": [],
        "Cena za m²": [],
        "Liczba pokoi": [],
        "Rynek": [],
        "Certyfikat energetyczny": [],
        "Numer mieszkania": [],
        "Rzut mieszkania": [],
        "Typ ogłoszeniodawcy": [],
        "Opis": [],
        "Rodzaj zabudowy": [],
        "Piętro": [],
        "Materiał budynku": [],
        "Okna": [],
        "Ogrzewanie": [],
        "Rok budowy": [],
        "Stan wykończenia": [],
        "Czynsz": [],
        "Forma własności": [],
        "Dostępne od": [],
        "Szerokość geograficzna": [],
        "Długość geograficzna": [],
    }


    
    for oferta in tqdm(lista_ofert, desc="getting data for offers "):

        # przygotowywanie html do parsowania
        url = "https://www.otodom.pl" + str(oferta)
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/"
        }
        r = requests.get(url = url, headers=headers)

        if r.status_code == 200:
            soup = BeautifulSoup(r.content, 'html5lib')
            html_string = str(soup)
        else:
            print(f"Failed to retrieve the webpage. Status code: {r.status_code}")

        data['link'].append(url)

        # Extract title
        title_tag = soup.find("title")
        data["Tytuł oferty"].append(
            title_tag.text.strip() if title_tag else "brak danych"
        )

        # Extract price
        price_tag = soup.find("meta", {"property": "og:description"})
        if price_tag:
            try:
                price_text = (
                    price_tag["content"].split("za cenę")[1].split(" zł")[0].strip()
                )
                data["Cena"].append(price_text)
            except IndexError:
                data["Cena"].append("brak danych")
        else:
            data["Cena"].append("brak danych")

        # Extract description
        description_meta = soup.find("meta", {"name": "description"})
        data["Opis"].append(
            description_meta["content"] if description_meta else "brak danych"
        )


        details_dict = {
            "Rynek": "Rynek",
            "Certyfikat energetyczny": "Certyfikat energetyczny",
            "Numer mieszkania": "Numer mieszkania",
            "Rzut mieszkania": "Rzut mieszkania",
            "Typ ogłoszeniodawcy": "Typ ogłoszeniodawcy",
            "Rodzaj zabudowy": "Rodzaj zabudowy",
            "Piętro": "Piętro",
            "Materiał budynku": "Materiał budynku",
            "Okna": "Okna",
            "Ogrzewanie": "Ogrzewanie",
            "Rok budowy": "Rok budowy",
            "Stan wykończenia": "Stan wykończenia",
            "Czynsz": "Czynsz",
            "Forma własności": "Forma własności",
            "Dostępne od": "Dostępne od",
        }


        #extracting values from table
        for key, label in details_dict.items():
            selector = f"p:-soup-contains('{label}')"  # Look for p tag containing the label
            try:
                element = soup.select_one(selector)
                if element:
                    value_element = element.find_next(
                        "p"
                    )  # Find the next p tag after the label
                    if value_element:
                        data[key].append(
                            value_element.text.strip()
                        )  #.append to collect to the list
                    else:
                        data[key].append("brak danych")
                else:
                    data[key].append("brak danych")
            except Exception as e:
                print(f"Error extracting {key}: {e}")
                data[key].append("brak danych")



        # Extract latitude and longitude
        coordinates_script = soup.find(
            "script", string=lambda text: text and '"__typename":"Coordinates"' in text
        )
        if coordinates_script:
            script_content = coordinates_script.string
            try:
                lat = (
                    script_content.split('"latitude":')[1].split(",")[0].strip()
                )
                lon = (
                    script_content.split('"longitude":')[1].split(",")[0].strip()
                )
            except IndexError:
                lat, lon = "brak danych", "brak danych"  # Set default
            data["Szerokość geograficzna"].append(lat)
            data["Długość geograficzna"].append(lon)
        else:
            data["Szerokość geograficzna"].append("brak danych")
            data["Długość geograficzna"].append("brak danych")

        # Extract area and price per square meter
        if description_meta:
            description = description_meta["content"]

            # Extract area
            try:
                area_text = description.split("ma ")[1].split(" m²")[0].strip()
                data["Powierzchnia"].append(area_text)
            except IndexError:
                data["Powierzchnia"].append("brak danych")

            # Extract rooms
            try:
                rooms_text = (
                    description.split("pokojowe")[0].strip().split(" ")[-1]
                )
                data["Liczba pokoi"].append(rooms_text)
            except IndexError:
                data["Liczba pokoi"].append("brak danych")

            # Calculate price per square meter
            try:
                price_text = data["Cena"][-1]  # Last price added
                price = float(price_text.replace(" ", "").replace(",", "."))
                area = float(area_text.replace(",", "."))
                data["Cena za m²"].append(round(price / area, 2))
            except (ValueError, TypeError):
                data["Cena za m²"].append("brak danych")

        else:
            data["Powierzchnia"].append("brak danych")
            data["Cena za m²"].append("brak danych")
            data["Liczba pokoi"].append("brak danych")


    data_set = pd.DataFrame(data)

    # data clean-up
    data_set['Cena'] = data_set['Cena'].astype(str).str.replace(',', '.')
    data_set['Cena'] = pd.to_numeric(data_set['Cena'].str.replace(' ', ''), errors='coerce')
    data_set['Powierzchnia'] = data_set['Powierzchnia'].str.replace(',', '.')
    data_set[['Piętro', 'liczba pięter w budynku']] = data_set['Piętro'].str.split('/', expand=True)
    data_set['Piętro'] = data_set['Piętro'].replace('parter', 0)
    data_set['Piętro'] = data_set['Piętro'].replace('> 10', 11) #wszystkie piętra wyżej niż 10 oznaczymy jako 11
    data_set['Czynsz'] = data_set['Czynsz'].str.replace(',', '.')
    data_set['Czynsz'] = data_set['Czynsz'].str.replace('zł', '')
    data_set = data_set.replace('brak danych', np.nan)
    data_set = data_set.replace('brak informacji', np.nan)
    data_set['Czynsz'] = data_set['Czynsz'].str.replace(' ', '')
    data_set['Cena za m²'] = pd.to_numeric(data_set['Cena za m²'], errors='coerce')

    # dropping not populated columns
    data_set = data_set.drop(['Rzut mieszkania', 'Numer mieszkania'], axis=1)
    kolejnosc = ['link','Tytuł oferty', 'Cena','Powierzchnia','Cena za m²', 'Liczba pokoi', 'Rynek', 'Piętro', 'liczba pięter w budynku', 'Rodzaj zabudowy','Rok budowy','Typ ogłoszeniodawcy', 'Certyfikat energetyczny','Rodzaj zabudowy','Materiał budynku','Okna','Ogrzewanie','Stan wykończenia', 'Czynsz', 'Forma własności', 'Dostępne od', 'Opis', 'Szerokość geograficzna', 'Długość geograficzna']
    data_set = data_set[kolejnosc]
    data_set = data_set.rename(columns={'Cena za m²': 'cena za metr', 'Szerokość geograficzna':'lat', 'Długość geograficzna':'lon'})
    data_set['Powierzchnia'] = pd.to_numeric(data_set['Powierzchnia'], errors='coerce')
    data_set['Rok budowy'] = pd.to_numeric(data_set['Rok budowy'], errors='coerce')
    data_set['Czynsz'] = pd.to_numeric(data_set['Czynsz'], errors='coerce')
    data_set['Piętro'] = pd.to_numeric(data_set['Piętro'], errors='coerce')
    data_set.drop_duplicates(subset='link')

    return data_set
