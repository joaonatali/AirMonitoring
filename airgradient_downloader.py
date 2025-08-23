import os
import json
from datetime import datetime, timedelta
from pathlib import Path
import polars as pl

import requests
from dotenv import load_dotenv

load_dotenv()

AIRGRADIENT_TOKEN = os.getenv("AIRGRADIENT_TOKEN")
AIRGRADIENT_LOCATION_ID = os.getenv("AIRGRADIENT_LOCATION_ID")


def test_airgradient_token() -> bool:
    """Tests the AirGradient API token."""
    if not AIRGRADIENT_TOKEN:
        raise ValueError("AIRGRADIENT_TOKEN must be set in the .env file.")

    print("Testing AirGradient API token...")
    url = "https://api.airgradient.com/public/api/v1/locations/measures/current"
    # url = "https://api.airgradient.com/public/api/v1/ping"
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
        # for loc in locations:
        #     print(f"  - Location ID: {loc['id']}, Name: {loc['name']}")
        return True
    except requests.exceptions.RequestException as e:
        print(f"Token is invalid or there was a problem connecting to the API: {e}")
        return False


def download_airgradient_data() -> None:
    """Downloads data from the AirGradient API and saves it to a CSV file."""

    if not AIRGRADIENT_TOKEN or not AIRGRADIENT_LOCATION_ID:
        raise ValueError(
            "AIRGRADIENT_TOKEN and AIRGRADIENT_LOCATION_ID must be set in the .env file."
        )

    print(f"\nUsing Location ID: {AIRGRADIENT_LOCATION_ID}")
    print(f"Using Token (first 5 chars): {AIRGRADIENT_TOKEN[:5]}...")

    # Get the current date for data range and filename
    now = datetime.now()

    url = f"https://api.airgradient.com/public/api/v1/locations/{AIRGRADIENT_LOCATION_ID}/measures/past"
    params = {
        "token": AIRGRADIENT_TOKEN,
        "from": (now - timedelta(days=10)).strftime("%Y%m%dT%H%M%SZ"),
        "to": now.strftime("%Y%m%dT%H%M%SZ"),
    }
    headers = {
        "Content-Type": "application/json",
    }

    print(f"Requesting URL: {url}")

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()  # Raise an exception for bad status codes
    except requests.exceptions.RequestException as e:
        print(f"Error downloading data: {e}")
        return

    data = response.json()
    print(type(data))

    if not data:
        print("No data returned from the API.")
        return

    df = pl.DataFrame(data)
    print(df)

    # Create the data directory if it doesn't exist
    data_dir = Path("data")
    data_dir.mkdir(exist_ok=True)

    # Create file named for currend date
    today = now.strftime("%Y-%m-%d")
    filename = data_dir / f"{today}.csv"

    # The API returns a single JSON object, not a list, so we need to handle it accordingly.
    df.write_csv(filename)

    print(f"Data successfully downloaded to {filename}")


if __name__ == "__main__":
    download_airgradient_data()
