import pandas as pd
from pandas import DataFrame as df
import requests
import csv_to_sqlite as csq


def get_fablabsio_json_db():
    url = "https://api.fablabs.io/0/labs.json"
    api_response = requests.get(
        url,
        allow_redirects=True,
        headers={"Content-Type": "application/json"},
    )
    if api_response.status_code == 200:
        print("Success getting Fablabs.io DB")
        json_file = api_response.json()
        return json_file
    else:
        print("Error " + str(api_response.status_code))
        return False


def json_csv_sqlite(json_db):
    dframe = df(json_db)
    dframe.to_csv("db/fablab_network.csv", encoding="utf-8")
    options = csq.CsvOptions(typing_style="full", encoding="utf-8")
    return csq.write_csv(["db/fablab_network.csv"], "db/fablab_network.db", options)


if __name__ == "__main__":
    json_db = get_fablabsio_json_db()
    json_csv_sqlite(json_db)
