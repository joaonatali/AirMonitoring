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
MOTHERDUCK_DB_NAME = os.getenv("MOTHERDUCK_DB_NAME")
MOTHERDUCK_TABLE_NAME = os.getenv("MOTHERDUCK_TABLE_NAME")

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


def upsert_dataframe_to_motherduck(
    df: pl.DataFrame,
    motherduck_token: str,
    db_name: str | None,
    table_name: str,
) -> bool:
    """Upserts a DataFrame to a specified table in MotherDuck."""
    if not motherduck_token:
        print("MOTHERDUCK_TOKEN not set. Skipping MotherDuck upload.")
        return False

    print(
        f"Pushing data to MotherDuck table {table_name} in database {db_name or 'default'}..."
    )

    try:
        df = df.with_columns(updated_at=datetime.now())
        db_string = f"md:{db_name}" if db_name else "md:"
        con = duckdb.connect(f"{db_string}?motherduck_token={motherduck_token}")

        create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
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
                updated_at TIMESTAMP,
                PRIMARY KEY (locationId, timestamp, serialno)
            )
        """
        con.execute(create_table_query)

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
        INSERT INTO {table_name}
        SELECT * FROM df
        ON CONFLICT (locationId, timestamp, serialno) DO UPDATE SET
        {update_clause}
        """
        con.execute(upsert_query)
        print(f"Data successfully upserted to MotherDuck table {table_name}.")
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
            if MOTHERDUCK_TOKEN and MOTHERDUCK_TABLE_NAME:
                upsert_dataframe_to_motherduck(
                    df, MOTHERDUCK_TOKEN, MOTHERDUCK_DB_NAME, MOTHERDUCK_TABLE_NAME
                )
            else:
                print(
                    "MOTHERDUCK_TOKEN and MOTHERDUCK_TABLE_NAME must be set to upload to MotherDuck."
                )


if __name__ == "__main__":
    app()
