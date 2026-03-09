from pytrends.request import TrendReq
import pandas as pd
from google.cloud import bigquery
from datetime import datetime
import time

PROJECT_ID = "arla-digital-intel-2025"
DATASET = "raw_ingest"


def fetch_dairy_brand_trends():
    """Weekly search interest: Arla vs plant-based competitors."""
    pytrends = TrendReq(hl="sv-SE", tz=60)
    all_rows = []

    keyword_groups = [
        ["Arla", "Oatly", "Alpro", "havredryck", "mjölk"],
        ["ekologisk mjölk", "laktosfri", "vegansk mat", "hållbar mat"],
    ]

    for keywords in keyword_groups:
        print(f"Fetching trends: {keywords}")
        try:
            pytrends.build_payload(
                keywords,
                timeframe="2019-01-01 2025-12-31",
                geo="SE"
            )
            df = pytrends.interest_over_time()
            if df.empty:
                continue
            df = df.drop(columns=["isPartial"], errors="ignore").reset_index()
            df_melted = df.melt(
                id_vars=["date"],
                var_name="keyword",
                value_name="search_interest"
            )
            df_melted["country_code"] = "SE"
            all_rows.append(df_melted)
            print(f"  {len(df_melted)} rows")
        except Exception as e:
            print(f"  Error: {e}")
        time.sleep(3)

    if not all_rows:
        return pd.DataFrame()
    result = pd.concat(all_rows, ignore_index=True)
    result["ingested_at"] = datetime.utcnow().isoformat()
    return result


def fetch_dairy_trends_by_region():
    """Regional search interest for key terms within Sweden."""
    pytrends = TrendReq(hl="sv-SE", tz=60)
    all_rows = []

    terms = ["Arla", "havredryck", "ekologisk mjölk"]

    for keyword in terms:
        print(f"Fetching regional trends: '{keyword}'")
        try:
            pytrends.build_payload(
                [keyword],
                timeframe="2024-01-01 2025-12-31",
                geo="SE"
            )
            df = pytrends.interest_by_region(resolution="REGION", inc_low_vol=True)
            if df.empty:
                continue
            df = df.reset_index()
            df.columns = ["region", "search_interest"]
            df["keyword"] = keyword
            df["country_code"] = "SE"
            all_rows.append(df)
            print(f"  {len(df)} regions")
        except Exception as e:
            print(f"  Error: {e}")
        time.sleep(3)

    if not all_rows:
        return pd.DataFrame()
    result = pd.concat(all_rows, ignore_index=True)
    result["ingested_at"] = datetime.utcnow().isoformat()
    return result


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
        (fetch_dairy_brand_trends, "google_trends_dairy_weekly"),
        (fetch_dairy_trends_by_region, "google_trends_dairy_regional"),
    ]:
        try:
            df = fn()
            if not df.empty:
                load_to_bigquery(df, table)
        except Exception as e:
            print(f"Error in {fn.__name__}: {e}")


if __name__ == "__main__":
    main()
