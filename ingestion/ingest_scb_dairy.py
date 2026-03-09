import requests
import pandas as pd
from google.cloud import bigquery
from datetime import datetime

PROJECT_ID = "arla-digital-intel-2025"
DATASET = "raw_ingest"


def fetch_scb_food_retail():
    """Fetch food retail trade index from Eurostat sts_trtu_m."""
    print("Fetching Sweden food retail index...")
    url = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/sts_trtu_m"
    params = {
        "geo": "SE",
        "s_adj": "NSA",
        "unit": "I21",
        "nace_r2": "G47.1",
        "format": "JSON",
        "lang": "en"
    }
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    time_index = data["dimension"]["time"]["category"]["index"]
    values = data["value"]
    rows = []
    for period, pos in time_index.items():
        value = values.get(str(pos))
        if value is not None:
            rows.append({
                "country_code": "SE",
                "period": period.replace("M", "-"),
                "index_value": float(value),
                "category": "food_retail",
                "source": "Eurostat"
            })
    print(f"Food retail: {len(rows)} rows")
    return pd.DataFrame(rows)


def fetch_scb_organic_sales():
    """Fetch organic food sales share from SCB PxWeb."""
    print("Fetching SCB organic food sales...")
    url = "https://api.scb.se/OV0104/v1/doris/en/ssd/HA/HA0103/HA0103A/EkoForsaljKv"
    try:
        meta = requests.get(url, timeout=30).json()
        variables = {v["code"]: v["values"] for v in meta.get("variables", [])}
        payload = {
            "query": [
                {"code": k, "selection": {"filter": "all", "values": ["*"]}}
                for k in variables
            ],
            "response": {"format": "json-stat2"}
        }
        r = requests.post(url, json=payload, timeout=60)
        r.raise_for_status()
        data = r.json()
        dims = list(data["dimension"].keys())
        time_dim = dims[-1]
        periods = list(data["dimension"][time_dim]["category"]["index"].keys())
        values = data.get("value", [])
        rows = []
        for period, value in zip(periods, values):
            if value is not None:
                rows.append({
                    "period": period,
                    "organic_sales_share_pct": float(value),
                    "source": "SCB"
                })
        print(f"Organic sales: {len(rows)} rows")
        return pd.DataFrame(rows)
    except Exception as e:
        print(f"SCB organic error: {e}")
        return pd.DataFrame()


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
        (fetch_scb_food_retail, "scb_food_retail_index"),
        (fetch_scb_organic_sales, "scb_organic_sales"),
    ]:
        try:
            df = fn()
            if not df.empty:
                load_to_bigquery(df, table)
        except Exception as e:
            print(f"Error in {fn.__name__}: {e}")


if __name__ == "__main__":
    main()
