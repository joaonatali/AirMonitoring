import os
from datetime import datetime, timedelta
from pathlib import Path
import polars as pl
import duckdb
import typer
from typing_extensions import Annotated

import requests
from dotenv import load_dotenv

load_dotenv()

AIRGRADIENT_TOKEN = os.getenv("AIRGRADIENT_TOKEN")
AIRGRADIENT_LOCATION_ID = os.getenv("AIRGRADIENT_LOCATION_ID")
MOTHERDUCK_TOKEN = os.getenv("MOTHERDUCK_TOKEN")

app = typer.Typer()


def test_airgradient_token() -> bool:
    """Tests the AirGradient API token."""
    if not AIRGRADIENT_TOKEN:
        raise ValueError("AIRGRADIENT_TOKEN must be set in the .env file.")

    print("Testing AirGradient API token...")
    url = "https://api.airgradient.com/public/api/v1/locations/measures/current"
    params = {
        "token": AIRGRADIENT_TOKEN,
    }
    headers = {
        "Content-Type": "application/json",
    }

    try:
        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status()
        print("Token is valid.")
        text = response.text
        print(f"Response text: {text}")
        return True
    except requests.exceptions.RequestException as e:
        print(f"Token is invalid or there was a problem connecting to the API: {e}")
        return False


def fetch_airgradient_data_to_dataframe(
    token: str, location_id: str
) -> pl.DataFrame | None:
    """Downloads data from the AirGradient API and returns a polars DataFrame."""
    print(f"\nUsing Location ID: {location_id}")
    print(f"Using Token (first 5 chars): {token[:5]}...")

    now = datetime.now()
    url = f"https://api.airgradient.com/public/api/v1/locations/{location_id}/measures/past"
    params = {
        "token": token,
        "from": (now - timedelta(days=10)).strftime("%Y%m%dT%H%M%SZ"),
        "to": now.strftime("%Y%m%dT%H%M%SZ"),
    }

    print(f"Requesting URL: {url}")

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Error downloading data: {e}")
        return None

    data = response.json()
    if not data:
        print("No data returned from the API.")
        return None

    df = pl.DataFrame(data)
    df = df.with_columns(pl.col("timestamp").str.to_datetime())
    print("Successfully fetched data into DataFrame.")
    return df


def save_dataframe_to_csv(df: pl.DataFrame) -> Path | None:
    """Saves a DataFrame to a CSV file."""
    try:
        data_dir = Path("data")
        data_dir.mkdir(exist_ok=True)
        today = datetime.now().strftime("%Y-%m-%d")
        filename = data_dir / f"{today}.csv"
        df.write_csv(filename)
        print(f"Data successfully saved to {filename}")
        return filename
    except Exception as e:
        print(f"Error saving DataFrame to CSV: {e}")
        return None


def upsert_dataframe_to_motherduck(df: pl.DataFrame, motherduck_token: str) -> bool:
    """Upserts a DataFrame to the airgradient_measures table in MotherDuck."""
    if not motherduck_token:
        print("MOTHERDUCK_TOKEN not set. Skipping MotherDuck upload.")
        return False

    print("Pushing data to MotherDuck...")
    try:
        con = duckdb.connect(f"md:?motherduck_token={motherduck_token}")
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS airgradient_measures (
                locationId INTEGER,
                locationName VARCHAR,
                pm01 DOUBLE,
                pm02 DOUBLE,
                pm10 DOUBLE,
                pm01_corrected DOUBLE,
                pm02_corrected DOUBLE,
                pm10_corrected DOUBLE,
                pm003Count DOUBLE,
                atmp DOUBLE,
                rhum DOUBLE,
                rco2 DOUBLE,
                atmp_corrected DOUBLE,
                rhum_corrected DOUBLE,
                rco2_corrected DOUBLE,
                tvoc DOUBLE,
                wifi DOUBLE,
                timestamp TIMESTAMP,
                serialno VARCHAR,
                model VARCHAR,
                firmwareVersion VARCHAR,
                tvocIndex DOUBLE,
                noxIndex INTEGER,
                datapoints INTEGER,
                PRIMARY KEY (locationId, timestamp, serialno)
            )
        """
        )

        column_names = df.columns
        primary_keys = ["locationId", "timestamp", "serialno"]
        update_clause = ", ".join(
            [
                f"{col} = excluded.{col}"
                for col in column_names
                if col not in primary_keys
            ]
        )

        upsert_query = f"""
        INSERT INTO airgradient_measures
        SELECT * FROM df
        ON CONFLICT (locationId, timestamp, serialno) DO UPDATE SET
        {update_clause}
        """
        con.execute(upsert_query)
        print("Data successfully upserted to MotherDuck.")
        return True
    except Exception as e:
        print(f"Error pushing data to MotherDuck: {e}")
        return False


@app.command()
def main(
    save_csv: Annotated[
        bool,
        typer.Option(
            help="Save the downloaded data to a CSV file.", rich_help_panel="Output"
        ),
    ] = False,
    to_motherduck: Annotated[
        bool,
        typer.Option(
            help="Upload the downloaded data to MotherDuck.", rich_help_panel="Output"
        ),
    ] = False,
) -> None:
    """Main function to download, save, and upload data."""
    if not AIRGRADIENT_TOKEN or not AIRGRADIENT_LOCATION_ID:
        raise ValueError(
            "AIRGRADIENT_TOKEN and AIRGRADIENT_LOCATION_ID must be set in the .env file."
        )

    df = fetch_airgradient_data_to_dataframe(AIRGRADIENT_TOKEN, AIRGRADIENT_LOCATION_ID)

    if df is not None:
        print(df.head(5))
        if save_csv:
            save_dataframe_to_csv(df)
        if to_motherduck:
            if MOTHERDUCK_TOKEN:
                upsert_dataframe_to_motherduck(df, MOTHERDUCK_TOKEN)
            else:
                print("MOTHERDUCK_TOKEN is not set. Cannot upload to MotherDuck.")


if __name__ == "__main__":
    app()
