
import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
import numpy as np
import concurrent.futures
import threading # Potrzebne do obsługi tqdm w wątkach
import os as os

# Zmienna globalna do przechowywania wyników, chroniona blokadą
# W tym przypadku nie jest to konieczne, jeśli zbieramy wyniki z Future,
# ale dobrze jest pamiętać o synchronizacji przy modyfikacji wspólnych zasobów.
# all_data_lock = threading.Lock() # Możesz tego użyć, jeśli dodajesz do globalnego słownika wewnątrz wątku

def fetch_and_parse_offer(oferta_path):
    """
    Pobiera dane dla pojedynczej oferty i zwraca słownik z danymi.
    Ta funkcja będzie wykonywana w osobnym wątku.
    """
    url = "https://www.otodom.pl" + str(oferta_path)
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/"
    }

    item_data = {
        "Tytuł oferty": "brak danych",
        "link": url,
        "Cena": "brak danych",
        "Powierzchnia": "brak danych",
        "Cena za m²": "brak danych",
        "Liczba pokoi": "brak danych",
        "Rynek": "brak danych",
        "Certyfikat energetyczny": "brak danych",
        "Numer mieszkania": "brak danych",
        "Rzut mieszkania": "brak danych",
        "Typ ogłoszeniodawcy": "brak danych",
        "Opis": "brak danych",
        "Rodzaj zabudowy": "brak danych",
        "Piętro": "brak danych",
        "Materiał budynku": "brak danych",
        "Okna": "brak danych",
        "Ogrzewanie": "brak danych",
        "Rok budowy": "brak danych",
        "Stan wykończenia": "brak danych",
        "Czynsz": "brak danych",
        "Forma własności": "brak danych",
        "Dostępne od": "brak danych",
        "Szerokość geograficzna": "brak danych",
        "Długość geograficzna": "brak danych",
    }

    try:
        r = requests.get(url=url, headers=headers, timeout=10) # Dodano timeout
        r.raise_for_status() # Wyrzuca wyjątek dla kodów statusu 4xx/5xx
        soup = BeautifulSoup(r.content, 'html5lib')

        # Extract title
        title_tag = soup.find("title")
        item_data["Tytuł oferty"] = title_tag.text.strip() if title_tag else "brak danych"

        # Extract price
        price_tag = soup.find("meta", {"property": "og:description"})
        if price_tag:
            try:
                price_text = (
                    price_tag["content"].split("za cenę")[1].split(" zł")[0].strip()
                )
                item_data["Cena"] = price_text
            except IndexError:
                pass # Pozostaw "brak danych"

        # Extract description
        description_meta = soup.find("meta", {"name": "description"})
        item_data["Opis"] = description_meta["content"] if description_meta else "brak danych"


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
            selector = f"p:-soup-contains('{label}')"
            try:
                element = soup.select_one(selector)
                if element:
                    value_element = element.find_next("p")
                    if value_element:
                        item_data[key] = value_element.text.strip()
            except Exception as e:
                # print(f"Error extracting {key} for {url}: {e}") # Ostrożnie z printami w wątkach
                pass


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
                item_data["Szerokość geograficzna"] = lat
                item_data["Długość geograficzna"] = lon
            except IndexError:
                pass
        
        # Extract area and price per square meter
        if description_meta:
            description = description_meta["content"]

            # Extract area
            try:
                area_text = description.split("ma ")[1].split(" m²")[0].strip()
                item_data["Powierzchnia"] = area_text
            except IndexError:
                pass

            # Extract rooms
            try:
                rooms_text = (
                    description.split("pokojowe")[0].strip().split(" ")[-1]
                )
                item_data["Liczba pokoi"] = rooms_text
            except IndexError:
                pass

            # Calculate price per square meter
            try:
                price_text = item_data["Cena"]
                area_text_for_calc = item_data["Powierzchnia"]
                price = float(price_text.replace(" ", "").replace(",", "."))
                area = float(area_text_for_calc.replace(",", "."))
                item_data["Cena za m²"] = round(price / area, 2)
            except (ValueError, TypeError, KeyError):
                pass

    except requests.exceptions.RequestException as e:
        print(f"Błąd połączenia dla {url}: {e}")
    except Exception as e:
        print(f"Niespodziewany błąd dla {url}: {e}")

    return item_data


def get_data_multithreaded(lista_ofert: list, max_threads = 16) -> pd.DataFrame:
    """Pobiera dane z listy ofert wielowątkowo i zwraca DataFrame."""

    results = []
    # Dobierz liczbę wątków - zazwyczaj 5-10 jest dobrym punktem wyjścia dla scraping
    # Zbyt duża liczba wątków może przeciążyć serwer docelowy lub Twoje łącze.
    # Optymalna liczba zależy od szybkości sieci, opóźnień serwera i limitów.
    # max_workers=os.cpu_count() * 2 to też często używana heurystyka dla I/O bound.

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
        # Mapowanie funkcji fetch_and_parse_offer na każdy element lista_ofert
        # executor.map jest prostsze, jeśli nie potrzebujesz dostępu do Future.
        # tqdm tutaj współpracuje z executor.map, aby pokazać postęp.
        # Pamiętaj, że tqdm może nie pokazywać postępu liniowo,
        # ponieważ zadania kończą się w różnej kolejności.
        futures = {executor.submit(fetch_and_parse_offer, oferta): oferta for oferta in lista_ofert}

        for future in tqdm(concurrent.futures.as_completed(futures), total=len(lista_ofert), desc="getting data for offers"):
            offer_url_path = futures[future] # Odzyskaj oryginalną ścieżkę oferty
            try:
                data_for_item = future.result()
                results.append(data_for_item)
            except Exception as exc:
                print(f'{offer_url_path} wygenerowało wyjątek: {exc}')

    # Inicjalizacja DataFrame na podstawie listy słowników
    data_set = pd.DataFrame(results)

    # --- Data Clean-up (przeniesione tutaj, po zebraniu wszystkich danych) ---
    # To jest CPU-bound i może pozostać w głównym wątku,
    # chyba że DataFrame jest bardzo duży i wymaga dalszej równoległości
    # (wtedy ProcessPoolExecutor dla tej części).

    data_set['Cena'] = data_set['Cena'].astype(str).str.replace(',', '.')
    data_set['Cena'] = pd.to_numeric(data_set['Cena'].str.replace(' ', ''), errors='coerce')
    data_set['Powierzchnia'] = data_set['Powierzchnia'].str.replace(',', '.')
    
    # Obsługa Piętro, która może nie mieć "/", zabezpieczenie przed błędem
    # Użycie .str.split().str[0] i .str.split().str[1]
    data_set['Piętro_temp'] = data_set['Piętro'].apply(lambda x: x.split('/')[0] if isinstance(x, str) and '/' in x else x)
    data_set['liczba pięter w budynku'] = data_set['Piętro'].apply(lambda x: x.split('/')[1] if isinstance(x, str) and '/' in x else np.nan)
    data_set['Piętro'] = data_set['Piętro_temp']
    data_set = data_set.drop(columns=['Piętro_temp'])


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
    data_set.drop_duplicates(subset='link', inplace=True) # dodano inplace=True

    return data_set