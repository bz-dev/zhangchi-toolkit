import json
import sys
from multiprocessing import Pool
from urllib.request import urlretrieve
from settings import BASE_DIR
from pathlib import Path
import pandas
import re


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


def worker(link):
    match = re.compile(".*([0-9]{4}-[0-9]{2}).*").match(link['Link'])
    year_month = match.group(1)
    dir_dest = BASE_DIR.joinpath('data', 'downloads', link['Region'])
    dir_dest.mkdir(parents=True, exist_ok=True)
    dest = BASE_DIR.joinpath('downloads', link['Region'], f'{year_month}-{link["Data"]}.csv.gz')
    if not dest.exists():
        try:
            urlretrieve(link['Link'], dest)
            link['result'] = 'downloaded'
            print(f'Downloaded: {link["Region"]} {year_month}-{link["Data"]}.csv.gz')
            return link
        except:
            link['result'] = 'error'
            print(f'Error: {link["Region"]} {year_month}-{link["Data"]}.csv.gz')
            return link
    else:
        link['result'] = 'exists'
        print(f'Exists: {link["Region"]} {year_month}-{link["Data"]}.csv.gz')
        return link


def download_from_list(path: str = None):
    try:
        path = sys.argv[1]
    except:
        pass
    if not path:
        print("File path required")
        return
    csv_fotis = Path(path)
    if csv_fotis.exists():
        df = pandas.read_csv(csv_fotis)
        links = df.to_dict('records')
        results = []
        with Pool(processes=12) as pool:
            for result in pool.imap_unordered(worker, links):
                results.append(result)
        df = pandas.DataFrame(results)
        df.to_csv(csv_fotis)




if __name__ == "__main__":
    download_from_list()