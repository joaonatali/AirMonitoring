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


def transform_legacy_schema(df: pl.DataFrame) -> pl.DataFrame:
    """Transforms a DataFrame with the legacy schema to the new schema."""
    schema_mapping = {
        "Location ID": "locationId",
        "Location Name": "locationName",
        "Sensor ID": "serialno",
        "UTC Date/Time": "timestamp",
        "# of aggregated records": "datapoints",
        "PM2.5 (μg/m³) raw": "pm02",
        "PM2.5 (μg/m³) corrected": "pm02_corrected",
        "0.3μm particle count": "pm003Count",
        "CO2 (ppm) raw": "rco2",
        "CO2 (ppm) corrected": "rco2_corrected",
        "Temperature (°C) raw": "atmp",
        "Temperature (°C) corrected": "atmp_corrected",
        "Humidity (%) raw": "rhum",
        "Humidity (%) corrected": "rhum_corrected",
        "TVOC (ppb)": "tvoc",
        "TVOC index": "tvocIndex",
        "NOX index": "noxIndex",
        "PM1 (μg/m³)": "pm01",
        "PM10 (μg/m³)": "pm10",
    }
    df = df.drop([col for col in df.columns if col not in schema_mapping.keys()])
    df = df.rename(schema_mapping)
    df = df.with_columns(pl.col("serialno").str.replace("airgradient:", ""))
    df = df.with_columns(
        [
            pl.lit(None, dtype=pl.Float64).alias("pm01_corrected"),
            pl.lit(None, dtype=pl.Float64).alias("pm10_corrected"),
            pl.lit(None, dtype=pl.Float64).alias("wifi"),
            pl.lit(None, dtype=pl.Utf8).alias("model"),
            pl.lit(None, dtype=pl.Utf8).alias("firmwareVersion"),
        ]
    )
    column_order = [
        "locationId",
        "locationName",
        "pm01",
        "pm02",
        "pm10",
        "pm01_corrected",
        "pm02_corrected",
        "pm10_corrected",
        "pm003Count",
        "atmp",
        "rhum",
        "rco2",
        "atmp_corrected",
        "rhum_corrected",
        "rco2_corrected",
        "tvoc",
        "wifi",
        "timestamp",
        "serialno",
        "model",
        "firmwareVersion",
        "tvocIndex",
        "noxIndex",
        "datapoints",
    ]
    df = df.select(column_order)
    return df


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
            if "Location ID" in df.columns:
                print("Legacy schema detected. Transforming...")
                df = transform_legacy_schema(df)

            df = df.with_columns(
                pl.col("timestamp").str.to_datetime().dt.replace_time_zone("UTC")
            )
            upsert_dataframe_to_motherduck(
                df, MOTHERDUCK_TOKEN, MOTHERDUCK_DB_NAME, MOTHERDUCK_TABLE_NAME
            )
        except Exception as e:
            print(f"Error processing file {csv_file}: {e}")


if __name__ == "__main__":
    main()
