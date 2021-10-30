import json
import sys
from urllib.request import urlretrieve
from settings import BASE_DIR


def __download__(url, dir_city, city, iso_date, file):
    date_texts = iso_date.split("-")
    dest = dir_city.joinpath(f"{date_texts[0]}-{date_texts[1]}-{file.strip()}")
    if not dest.exists():
        print(f"Downloading for {city}: {url}")
        try:
            urlretrieve(url, dest)
        except:
            print(f"Failed to download for {city}: {url}")
    else:
        print(f"File already exists for {city}: {url}")


def download_city(city: str = None):
    try:
        city = sys.argv[1]
    except:
        pass
    if not city:
        print("City name required")
    dir_cities = BASE_DIR.joinpath("data", "cities")
    dir_dest = BASE_DIR.joinpath("data", "downloads")
    city_json = dir_cities.joinpath(f"{city}.json")
    data_items = []
    if city_json.exists():
        with open(city_json, "r") as f:
            data_items = json.load(f)
    for data_item in data_items:
        dir_city = dir_dest.joinpath(city)
        dir_city.mkdir(parents=True, exist_ok=True)
        __download__(data_item["url"], dir_city, data_item["city"], data_item["date"], data_item["file"])


if __name__ == "__main__":
    download_city()