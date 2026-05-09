
import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
import numpy as np
import concurrent.futures
import threading # Potrzebne do obsługi tqdm w wątkach
import os as os
import json

# Zmienna globalna do przechowywania wyników, chroniona blokadą
# W tym przypadku nie jest to konieczne, jeśli zbieramy wyniki z Future,
# ale dobrze jest pamiętać o synchronizacji przy modyfikacji wspólnych zasobów.
# all_data_lock = threading.Lock() # Możesz tego użyć, jeśli dodajesz do globalnego słownika wewnątrz wątku

def fetch_and_parse_offer(oferta_path):
    url = "https://www.otodom.pl" + str(oferta_path)
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "pl-PL,pl;q=0.9",
    }

    item_data = {
        "Tytuł oferty": None,
        "link": url,
        "Cena": None,
        "Powierzchnia": None,
        "Cena za m²": None,
        "Liczba pokoi": None,
        "Rynek": None,
        "Certyfikat energetyczny": None,
        "Typ ogłoszeniodawcy": None,
        "Opis": None,
        "Rodzaj zabudowy": None,
        "Piętro": None,
        "Materiał budynku": None,
        "Okna": None,
        "Ogrzewanie": None,
        "Rok budowy": None,
        "Stan wykończenia": None,
        "Czynsz": None,
        "Forma własności": None,
        "Dostępne od": None,
        "Szerokość geograficzna": None,
        "Długość geograficzna": None,
    }

    # Mapowanie kluczy z JSON-a otodom na kolumny
    CHARACTERISTICS_MAP = {
        "market":                "Rynek",
        "building_type":         "Rodzaj zabudowy",
        "building_material":     "Materiał budynku",
        "windows_type":          "Okna",
        "heating":               "Ogrzewanie",
        "build_year":            "Rok budowy",
        "construction_status":   "Stan wykończenia",
        "building_ownership":    "Forma własności",
        "free_from":             "Dostępne od",
        "energy_certificate":    "Certyfikat energetyczny",
        "rooms_num":             "Liczba pokoi",
        "m":                     "Powierzchnia",
        "building_floors_num":   "liczba pięter w budynku",
    }

    try:
        r = requests.get(url=url, headers=headers, timeout=10)
        r.raise_for_status()
        soup = BeautifulSoup(r.content, "html.parser")

        next_data_tag = soup.find("script", {"id": "__NEXT_DATA__"})
        if not next_data_tag:
            print(f"Brak __NEXT_DATA__ dla {url}")
            return item_data

        next_data = json.loads(next_data_tag.string)
        ad = next_data.get("props", {}).get("pageProps", {}).get("ad", {})

        if not ad:
            print(f"Brak danych ad dla {url}")
            return item_data

        # Tytuł i opis
        item_data["Tytuł oferty"] = ad.get("title")
        item_data["Opis"] = ad.get("description")

        # Typ ogłoszeniodawcy
        item_data["Typ ogłoszeniodawcy"] = ad.get("advertiserType")

        # Koordynaty
        coordinates = (
            ad.get("location", {}).get("coordinates", {})
        )
        item_data["Szerokość geograficzna"] = coordinates.get("latitude")
        item_data["Długość geograficzna"] = coordinates.get("longitude")

        # Charakterystyki — budujemy słownik key -> value
        characteristics = ad.get("characteristics", [])
        char_dict = {c["key"]: c.get("value") for c in characteristics}

        # Cena i cena za m² — bierzemy gotowe wartości z JSON
        item_data["Cena"] = char_dict.get("price")
        item_data["Cena za m²"] = char_dict.get("price_per_m")
        item_data["Czynsz"] = char_dict.get("rent")

        # Mapowanie pozostałych pól
        for json_key, col_name in CHARACTERISTICS_MAP.items():
            if json_key in char_dict:
                item_data[col_name] = char_dict[json_key]

        # Piętro — wyciągnij liczbę z "floor_9" -> 9, "floor_10" -> 10
        floor_raw = char_dict.get("floor_no")
        if floor_raw:
            if floor_raw == "ground_floor":
                item_data["Piętro"] = 0
            else:
                # "floor_9" -> "9"
                item_data["Piętro"] = floor_raw.replace("floor_", "")

    except requests.exceptions.RequestException as e:
        print(f"Błąd połączenia dla {url}: {e}")
    except json.JSONDecodeError as e:
        print(f"Błąd parsowania JSON dla {url}: {e}")
    except Exception as e:
        print(f"Niespodziewany błąd dla {url}: {e}")

    return item_data

def get_data_multithreaded(lista_ofert: list, max_threads=16) -> pd.DataFrame:
    results = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
        futures = {
            executor.submit(fetch_and_parse_offer, oferta): oferta
            for oferta in lista_ofert
        }
        for future in tqdm(
            concurrent.futures.as_completed(futures),
            total=len(lista_ofert),
            desc="Getting data for offers",
        ):
            try:
                results.append(future.result())
            except Exception as exc:
                print(f"{futures[future]} wygenerowało wyjątek: {exc}")

    data_set = pd.DataFrame(results)

    # --- Czyszczenie ---
    data_set["Cena"] = pd.to_numeric(
        data_set["Cena"].astype(str).str.replace(r"[\s,]", "", regex=True),
        errors="coerce",
    )
    data_set["Powierzchnia"] = pd.to_numeric(
        data_set["Powierzchnia"].astype(str).str.replace(",", "."),
        errors="coerce",
    )
    data_set["Piętro"] = (
        data_set["Piętro"]
        .replace("parter", "0")
        .replace("> 10", "11")
    )
    data_set["Piętro"] = pd.to_numeric(data_set["Piętro"], errors="coerce")
    data_set["Czynsz"] = pd.to_numeric(
        data_set["Czynsz"].astype(str).str.replace(r"[zł\s,]", "", regex=True),
        errors="coerce",
    )
    data_set["Rok budowy"] = pd.to_numeric(data_set["Rok budowy"], errors="coerce")
    data_set["cena za metr"] = pd.to_numeric(data_set["Cena za m²"], errors="coerce")
    data_set["lat"] = pd.to_numeric(data_set["Szerokość geograficzna"], errors="coerce")
    data_set["lon"] = pd.to_numeric(data_set["Długość geograficzna"], errors="coerce")

    data_set.replace("brak danych", np.nan, inplace=True)
    data_set.replace("brak informacji", np.nan, inplace=True)

    kolejnosc = [
        "link", "Tytuł oferty", "Cena", "Powierzchnia", "cena za metr",
        "Liczba pokoi", "Rynek", "Piętro", "liczba pięter w budynku",
        "Rodzaj zabudowy", "Rok budowy", "Typ ogłoszeniodawcy",
        "Certyfikat energetyczny", "Materiał budynku", "Okna", "Ogrzewanie",
        "Stan wykończenia", "Czynsz", "Forma własności", "Dostępne od",
        "Opis", "lat", "lon",
    ]
    # Zostaw tylko kolumny które istnieją (zabezpieczenie)
    kolejnosc = [c for c in kolejnosc if c in data_set.columns]
    data_set = data_set[kolejnosc]
    data_set.drop_duplicates(subset="link", inplace=True)

    return data_set