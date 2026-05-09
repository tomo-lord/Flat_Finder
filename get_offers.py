import requests
from bs4 import BeautifulSoup
from tqdm import tqdm


def get_offers(
    transaction_type: str = "sprzedaz",
    category: str = "mieszkanie",
    region: str = "mazowieckie",
    city: str = "warszawa",
    subregion: str = "warszawa",
    district: str = "warszawa",
    pages: int = 50,
    extra_params: dict | None = None,
) -> list[str]:
    base_url = (
        f"https://www.otodom.pl/pl/wyniki"
        f"/{transaction_type.lower()}/{category.lower()}/{region.lower()}/{city.lower()}/{subregion.lower()}/{district.lower()}"
    )
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "pl-PL,pl;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }

    lista_ofert: list[str] = []
    seen: set[str] = set()

    with requests.Session() as session:
        session.headers.update(headers)

        for page in tqdm(range(1, pages + 1), desc="Szukanie ofert"):
            params = extra_params.copy() if extra_params else {}

            if page > 1:
                params["page"] = page

            if page > 1:
                session.headers.update({"Referer": f"{base_url}?page={page}"})

            try:
                r = session.get(base_url, params=params, timeout=10)
                r.raise_for_status()
            except requests.exceptions.RequestException as e:
                print(f"Błąd na stronie {page}: {e}")
                break


            soup = BeautifulSoup(r.content, "html.parser")

            new_paths = [
                link["href"]
                for link in soup.find_all("a", href=True)
                if link["href"].startswith("/pl/oferta/")
            ]

            added_count = 0
            for path in new_paths:
                if path not in seen:
                    seen.add(path)
                    lista_ofert.append(path)
                    added_count += 1

            if added_count == 0:
                print(f"Brak nowych ofert na stronie {page} — zatrzymuję.")
                break

    print(f"Łączna liczba ofert: {len(lista_ofert)}")
    return lista_ofert


if __name__ == "__main__":
    oferty = get_offers(
        district="warszawa",
        pages=5,
            )
    #print(oferty)