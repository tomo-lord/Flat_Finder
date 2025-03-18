import requests
import re
import pandas as pd
from bs4 import BeautifulSoup
from IPython.display import clear_output




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


    
    counter = 0
    for oferta in lista_ofert:

        # przygotowywanie html do parsowania
        url = "https://www.otodom.pl/" + str(oferta)
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/"
        }
        r = requests.get(url = url, headers=headers)

        if r.status_code == 200:
            soup = BeautifulSoup(r.content, 'html5lib')
            html_string = str(soup)
            clear_output(wait=False)
            counter += 1
            print(f"Proggress: {counter} out of " + str(len(lista_ofert)))
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
            selector = f"p:contains('{label}')"  # Look for p tag containing the label
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


    df = pd.DataFrame(data)
    return df
