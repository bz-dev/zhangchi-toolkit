import re
import sys
from typing import Union, List, Optional
from pandas import DataFrame
from settings import BASE_DIR
import pandas
from pandas._libs.missing import NAType
from tqdm import tqdm

DEFAULT_LISTING_FIELDS = ["id", "accommodates", "host_response_rate", "review_scores_rating"]
DEFAULT_CALENDAR_FIELDS = ["listing_id", "date", "available", "price"]


def reduce_data(
        source: str,
        city: str,
        year: Union[int, str],
        month: Union[int, str],
        fields: List[str] = None,
) -> Optional[DataFrame]:
    """
    Read listings or calendar data and retain only required fields
    """

    # Check for valid month input
    if not 1 <= int(month) <= 12:
        raise ValueError(
            f"Invalid month value. Expecting an integer value between 1 and 12. Received {month}"
        )
    # Check for valid data source type
    if source.lower() not in ["listings", "calendar"]:
        raise ValueError(
            f"Invalid data source type. Expecting listing or calendar. Received {source}"
        )
    path = (
        BASE_DIR.joinpath("data", "downloads", city)
            .joinpath(f"{year}-{str(int(month)).zfill(2)}-{source}.csv.gz")
    )
    if not path.exists():
        return None
    df = pandas.read_csv(path, thousands=",")
    if fields:
        df = df[fields]
    print(df.columns)
    return df


def clean_price(price: Union[NAType, float, int, str]) -> Union[NAType, float]:
    """
    Clean price field, remove currency sign, comma and blank spaces
    """
    if pandas.isna(price):
        return price
    return float(str(price).strip().lstrip("$").strip().replace(",", ""))


def percent_converter(x):
    if isinstance(x, str):
        return float(x.strip('%')) / 100
    return x


def aggregate_prices(df):
    return (
        df[df["available"] == "t"]
            .groupby("date")
            .agg(
            min_price=("price", "min"),
            max_price=("price", "max"),
            mean_price=("price", "mean"),
            std_price=("price", "std"),
            total_accommodates=("accommodates", "sum"),
        )
    )


def process_month(
        city: str,
        year: Union[int, str],
        month: Union[int, str],
        fields_listing: List[str] = None,
        fields_calendar: List[str] = None,
):
    """
    Generate monthly data for given year and month.
    Result columns: date,available,total,available_ratio,min_price,max_price,mean_price,std_price,total_accommodates
    """
    if fields_listing is None:
        fields_listing = DEFAULT_LISTING_FIELDS
    if fields_calendar is None:
        fields_calendar = DEFAULT_CALENDAR_FIELDS
    listings = reduce_data("listings", city, year, month, fields_listing).rename(
        columns={"id": "listing_id"}
    )
    listings["host_response_rate"] = listings["host_response_rate"].apply(percent_converter)
    listings["review_scores_rating"] = listings["review_scores_rating"].apply(percent_converter)
    listings = listings[(listings["host_response_rate"] >= 0.85) & (listings["review_scores_rating"] >= 0.85)]
    calendars = reduce_data("calendar", city, year, month, fields_calendar)
    calendars = pandas.merge(calendars, listings, how="left", on="listing_id")
    calendars["price"] = calendars["price"].apply(clean_price)
    calendars_total = calendars.groupby("date").size().reset_index(name="total")
    calendars_available = (
        calendars[calendars["available"] == "t"]
            .groupby("date")
            .size()
            .reset_index(name="available")
    )
    calendars_ratio = pandas.merge(
        calendars_available, calendars_total, how="left", on="date"
    )
    calendars_ratio["available_ratio"] = (
            calendars_ratio["available"] / calendars_ratio["total"]
    )
    # This is where you can modify aggregation functions
    # See https://pandas.pydata.org/pandas-docs/stable/user_guide/groupby.html#aggregation for more available functions
    calendars_price = aggregate_prices(calendars)
    calendars = pandas.merge(calendars, calendars_price, how="left", on="date")
    calendars = calendars[calendars["price"] >= calendars["mean_price"]]
    calendars_price = aggregate_prices(calendars)
    calendars_ratio = pandas.merge(
        calendars_ratio, calendars_price, how="left", on="date"
    )
    calendars_ratio["booked"] = calendars_ratio["total"] - calendars_ratio["available"]
    path = (
        BASE_DIR.joinpath("data", "result", city, "monthly")
            .joinpath(f"{year}-{str(month).zfill(2)}-processed.csv")
    )
    if not path.parent.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
    calendars_ratio.to_csv(path, index=False)
    return calendars_ratio


def process_all_months(city,
                       fields_listing: List[str] = None,
                       fields_calendar: List[str] = None):
    if fields_listing is None:
        fields_listing = DEFAULT_LISTING_FIELDS
    if fields_calendar is None:
        fields_calendar = DEFAULT_CALENDAR_FIELDS
    source_dir = BASE_DIR.joinpath("data", "downloads", city)
    for f in tqdm(source_dir.glob("*.csv.gz")):
        match = re.compile("(\d{4})-(\d{2})-[a-z]+\.csv\.gz").match(f.name)
        if match:
            process_month(
                city,
                match.group(1),
                match.group(2),
                fields_listing,
                fields_calendar
            )


def get_specific_day(
        city: str,
        month: int,
        day: int,
        process_again: bool = True,
        fields_listing: List[str] = None,
        fields_calendar: List[str] = None,
):
    if fields_listing is None:
        fields_listing = DEFAULT_LISTING_FIELDS
    if fields_calendar is None:
        fields_calendar = DEFAULT_CALENDAR_FIELDS
    result_dir = BASE_DIR.joinpath("data", "result", city, "monthly")
    result_dfs = []
    if not result_dir.exists() or process_again:
        source_dir = BASE_DIR.joinpath("data", "downloads", city)
        for f in tqdm(source_dir.glob("*.csv.gz")):
            match = re.compile("(\d{4})-(\d{2})-[a-z]+\.csv\.gz").match(f.name)
            if match:
                month_data = process_month(
                    city,
                    match.group(1),
                    match.group(2),
                    fields_listing,
                    fields_calendar,
                )
                month_data["calendar_file_date"] = month_data["date"].iloc[0]
                month_data = month_data[
                    month_data["date"].str.endswith(
                        f"{str(month).zfill(2)}-{str(day).zfill(2)}"
                    )
                ]
                result_dfs.append(month_data)
    else:
        for f in tqdm(result_dir.glob("*processed.csv")):
            month_data = pandas.read_csv(f)
            month_data["calendar_file_date"] = month_data["date"].iloc[0]
            month_data = month_data[
                month_data["date"].str.endswith(
                    f"{str(month).zfill(2)}-{str(day).zfill(2)}"
                )
            ]
            result_dfs.append(month_data)
    df = pandas.concat(result_dfs, ignore_index=True)
    df["date"] = pandas.to_datetime(df["date"], format="%Y-%m-%d")
    return df


def generate_date_vintage(city):
    monthly_dir = BASE_DIR.joinpath("data", "result", city, "monthly")
    if not monthly_dir.exists():
        process_all_months(city)
    df_result = None
    for f in tqdm(monthly_dir.glob("*processed.csv")):
        df = pandas.read_csv(f)
        df.index = pandas.to_datetime(df["date"], format="%Y-%m-%d")
        df = df.groupby(pandas.Grouper(freq="M")).sum()[["available", "booked", "total"]]
        df = df.reset_index()
        df["year-month"] = df.date.apply(lambda xd: f"{xd.year}-{xd.month}")
        ym_start = df["year-month"][0]
        print(df.columns)
        df = df.rename(columns={"booked": ym_start})
        df = df.set_index("year-month")
        df = df[[ym_start]]
        print(df)
        if df_result is None:
            df_result = df
        else:
            df_result = pandas.concat([df_result, df], axis=1)
        print(df_result)
    output_path = BASE_DIR.joinpath("data", "result", city, "vintage", f"{city}-vintage-all.csv")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df_result.to_csv(output_path)




if __name__ == "__main__":
    if sys.argv[1] == "month":
        process_month(city=sys.argv[2], year=int(sys.argv[3]), month=int(sys.argv[4]))
    elif sys.argv[1] == "day":
        df = get_specific_day(city=sys.argv[2], month=int(sys.argv[3]), day=int(sys.argv[4]))
        print(df)
    elif sys.argv[1] =="allmonths":
        process_all_months(city=sys.argv[2])
    elif sys.argv[1] =="vintage":
        generate_date_vintage(city=sys.argv[2])
