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
        "nace_r2": "G47",
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
    """Fetch organic food market share from Eurostat org_cropar_gen."""
    print("Fetching organic food data...")
    url = f"https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/org_cropar_gen"
    params = {"geo": "SE", "format": "JSON", "lang": "en"}
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
                "year": int(period),
                "organic_area_ha": float(value),
                "source": "Eurostat"
            })
    print(f"Organic farming: {len(rows)} rows")
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

