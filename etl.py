import requests
import json
import duckdb
import os
from datetime import datetime
import pandas as pd
import modal

stub = modal.Stub("weatherstocking")
if modal.is_local():
    cities = pd.read_csv("cities.csv").to_json(orient="records")
    global_vars = {"cities": json.loads(cities)}
    stub.variables_dict = modal.Dict.new(global_vars)

modal_image = modal.Image.debian_slim(python_version="3.10").run_commands(
    "pip install requests", "pip install duckdb==0.8.1", "pip install pandas==1.3.4"
)


def dump_url_to_json(url, json_file):
    response = requests.get(url)
    data = response.json()
    with open(json_file, "a") as f:
        json.dump(data, f)
        f.write("\n")


def extract_and_load_data(con, api_key, table="weather"):
    update_time = datetime.now().strftime("%Y-%d-%mT%H:%M:%S")
    file_name = f"{update_time}_{table}"
    json_file = f"{file_name}.json"
    s3_file = f"s3://weatherstocking/{file_name}.parquet"

    for city in stub.app.variables_dict["cities"]:
        url = f"https://api.openweathermap.org/data/2.5/" \
            f"{table}?lat={city['lat']}&lon={city['lon']}&appid={api_key}"
        dump_url_to_json(url, json_file)

    con.execute(
        f"create or replace table {table} as select *"\
            f"from read_json_auto('{json_file}');"
    )

    con.execute(f"copy {table} to '{s3_file}'; drop table {table};")

    os.remove(json_file)


@stub.function(
    image=modal_image,
    schedule=modal.Cron("0 */3 * * *"),
    secret=modal.Secret.from_name("weatherstocking_secrets"),
)
def obtain_weather():
    duckdb_con = duckdb.connect()
    duckdb_con.execute(
        f"""install httpfs;
        load httpfs;
        set s3_endpoint='nyc3.digitaloceanspaces.com';
        set s3_region='nyc3';
        set s3_access_key_id='{os.environ["S3_ACCESS_KEY_ID"]}';
        set s3_secret_access_key='{os.environ["S3_SECRET_ACCESS_KEY"]}';"""
    )
    extract_and_load_data(
        duckdb_con, api_key=os.environ["OPENWEATHER_API_KEY"], table="weather"
    )
    extract_and_load_data(
        duckdb_con, api_key=os.environ["OPENWEATHER_API_KEY"], table="forecast"
    )
    duckdb_con.close()


if __name__ == "__main__":
    modal.runner.deploy_stub(stub)
