import requests
import pandas as pd
from google.cloud import bigquery
from datetime import datetime
import pandasdmx as sdmx

PROJECT_ID = "arla-digital-intel-2025"
DATASET = "raw_ingest"
BASE = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data"



import requests
import pandas as pd

import requests
import pandas as pd

import requests
import pandas as pd

def fetch_milk_collection():
    """Eurostat apro_mk_pobta via SDMX 2.1 — milk production Sweden."""
    print("Fetching milk collection data...")
    url = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/apro_mk_pobta"
    params = {
        "format": "JSON",
        "lang": "EN",
        "geo": "SE",
        "startPeriod": "2000",
        "endPeriod": "2025"
    }
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()

    structure = data["data"]["structures"][0]
    series_dims = structure["dimensions"]["series"]
    obs_dims = structure["dimensions"]["observation"][0]["values"]
    all_series = data["data"]["dataSets"][0]["series"]

    rows = []
    for series_key, series_data in all_series.items():
        indices = [int(i) for i in series_key.split(":")]
        # extract unit and dairyprod labels for context
        dim_labels = {
            dim["id"]: dim["values"][indices[i]]["id"]
            for i, dim in enumerate(series_dims)
        }
        for obs_idx, obs_values in series_data["observations"].items():
            value = obs_values[0]
            if value is not None:
                rows.append({
                    "country_code": "SE",
                    "year": obs_dims[int(obs_idx)]["id"],
                    "dairyprod": dim_labels.get("dairyprod", ""),
                    "unit": dim_labels.get("unit", ""),
                    "value": float(value),
                    "source": "Eurostat_SDMX"
                })

    print(f"Milk data: {len(rows)} rows")
    return pd.DataFrame(rows)


def fetch_internet_activities_by_age():
    """Eurostat isoc_ci_ac_i — internet activities by age group for Sweden."""
    print("Fetching internet activities by age...")
    url = f"{BASE}/isoc_ci_ac_i"
    params = {
        "geo": "SE",
        "ind_type": ["Y16_24", "Y25_34", "Y35_44", "Y45_54", "Y55_64", "Y65_74"],
        "format": "JSON",
        "lang": "en"
    }
    r = requests.get(url, params=params, timeout=60)
    r.raise_for_status()
    data = r.json()

    dims = data["dimension"]
    ind_index = dims["ind_type"]["category"]["index"]
    indic_index = dims["indic_is"]["category"]["index"]
    time_index = dims["time"]["category"]["index"]

    n_ind = len(ind_index)
    n_indic = len(indic_index)
    n_time = len(time_index)

    ind_list = list(ind_index.keys())
    indic_list = list(indic_index.keys())
    time_list = list(time_index.keys())

    values = data.get("value", {})
    rows = []
    for i, ind in enumerate(ind_list):
        for j, indic in enumerate(indic_list):
            for k, year in enumerate(time_list):
                pos = i * (n_indic * n_time) + j * n_time + k
                value = values.get(str(pos))
                if value is not None:
                    rows.append({
                        "country_code": "SE",
                        "age_group": ind,
                        "activity": indic,
                        "year": int(year),
                        "pct_individuals": float(value),
                        "source": "Eurostat"
                    })
    print(f"Internet activities: {len(rows)} rows")
    return pd.DataFrame(rows)


def fetch_online_buying_by_age():
    """Eurostat isoc_ec_ibuy — % individuals buying online by age group, Sweden."""
    print("Fetching online buying by age...")
    url = f"{BASE}/isoc_ec_ibuy"
    params = {
        "geo": "SE",
        "ind_type": ["Y16_24", "Y25_34", "Y35_44", "Y45_54", "Y55_64", "Y65_74"],
        "indic_is": "I_BLT12",
        "unit": "PC_IND",
        "format": "JSON",
        "lang": "en"
    }
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()

    dims = data["dimension"]
    ind_index = dims["ind_type"]["category"]["index"]
    time_index = dims["time"]["category"]["index"]
    ind_list = list(ind_index.keys())
    time_list = list(time_index.keys())
    n_time = len(time_list)
    values = data.get("value", {})

    rows = []
    for i, ind in enumerate(ind_list):
        for k, year in enumerate(time_list):
            pos = i * n_time + k
            value = values.get(str(pos))
            if value is not None:
                rows.append({
                    "country_code": "SE",
                    "age_group": ind,
                    "year": int(year),
                    "pct_buying_online": float(value),
                    "source": "Eurostat"
                })
    print(f"Online buying by age: {len(rows)} rows")
    return pd.DataFrame(rows)


def load_to_bigquery(df, table_name):
    client = bigquery.Client(project=PROJECT_ID)
    table_id = f"{PROJECT_ID}.{DATASET}.{table_name}"
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        autodetect=True
    )
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    print(f"Loaded {len(df)} rows into {table_id}")


def main():
    for fn, table in [
        (fetch_milk_collection, "eurostat_milk_collection"),
        (fetch_internet_activities_by_age, "eurostat_internet_activities_age"),
        (fetch_online_buying_by_age, "eurostat_online_buying_age"),
    ]:
        try:
            df = fn()
            if not df.empty:
                load_to_bigquery(df, table)
        except Exception as e:
            print(f"Error in {fn.__name__}: {e}")


if __name__ == "__main__":
    main()










