import os
from pathlib import Path
import polars as pl
from dotenv import load_dotenv

from airgradient_downloader import upsert_dataframe_to_motherduck

load_dotenv()

MOTHERDUCK_TOKEN = os.getenv("MOTHERDUCK_TOKEN")
MOTHERDUCK_DB_NAME = os.getenv("MOTHERDUCK_DB_NAME")
MOTHERDUCK_TABLE_NAME = os.getenv("MOTHERDUCK_TABLE_NAME")


def get_csv_files(data_dir: str = "data") -> list[Path]:
    """Gets all CSV files from the data directory."""
    data_path = Path(data_dir)
    if not data_path.is_dir():
        print(f"Data directory not found: {data_dir}")
        return []
    return list(data_path.glob("*.csv"))


def main() -> None:
    """Main function to read CSVs and upload data to MotherDuck."""
    if not MOTHERDUCK_TOKEN or not MOTHERDUCK_TABLE_NAME:
        raise ValueError(
            "MOTHERDUCK_TOKEN and MOTHERDUCK_TABLE_NAME must be set in the .env file."
        )

    csv_files = get_csv_files()
    if not csv_files:
        print("No CSV files found to upload.")
        return

    for csv_file in csv_files:
        print(f"Processing file: {csv_file}")
        try:
            df = pl.read_csv(csv_file)
            df = df.with_columns(pl.col("timestamp").str.to_datetime())
            upsert_dataframe_to_motherduck(
                df, MOTHERDUCK_TOKEN, MOTHERDUCK_DB_NAME, MOTHERDUCK_TABLE_NAME
            )
        except Exception as e:
            print(f"Error processing file {csv_file}: {e}")


if __name__ == "__main__":
    main()
