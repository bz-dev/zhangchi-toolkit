import scrapy
import re
from datetime import datetime
import json
from utils import update_workflow
from settings import BASE_DIR


class InsideAirbnbSpider(scrapy.Spider):
    name = "inside_airbnb"
    start_urls = ["http://insideairbnb.com/get-the-data.html"]

    def parse(self, response, **kwargs):
        dir_data = BASE_DIR.joinpath("data", "cities")
        tables = response.xpath("//table")
        for table in tables:
            classes = table.attrib['class']
            match = re.match("table table-hover table-striped (.*)", classes)
            if match:
                city = match.group(1)
                data = []
                file_city = dir_data.joinpath(f"{city}.json")
                print(f"Processing data for: {match.group(1)}")
                tbody = table.xpath("tbody")
                rows = tbody.xpath("tr")
                for index, row in enumerate(rows):
                    if row.xpath("td[1]//text()").get() != "N/A":
                        data.append({
                            "url": row.xpath("td[3]/a").attrib['href'],
                            "date": datetime.strptime(row.xpath("td[1]//text()").get(), "%d %B, %Y").strftime(
                                "%Y-%m-%d"),
                            "city": city,
                            "file": row.xpath("td[3]//text()").get()
                        })
                with open(file_city, "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=4)
        update_workflow()
