# Gemini Project Context

This file provides context about the AirMonitoring project for the Gemini AI assistant.

## Project Goal

The primary goal of this project is to download and analyze air quality data from a personal AirMonitoring sensor.

## Authentication

- **Method:** The project uses the AirMonitoring Public API, which requires an API token for authentication.
- **Storage:** The API token is stored in a `.env` file in the project root.
- **Variable:** The token should be assigned to the `AIRGRADIENT_TOKEN` environment variable.

## Data Fetching

- **Identifier:** Data is fetched using a specific `locationId`.
- **Storage:** The location ID is also stored in the `.env` file.
- **Variable:** The location ID should be assigned to the `AIRGRADIENT_LOCATION_ID` environment variable.
- **Script:** The `airgradient_downloader.py` script is responsible for fetching the data. It can be run as a CLI application.
- **Usage:**
    - `uv run python airgradient_downloader.py`: Downloads data and prints the first 5 rows.
    - `uv run python airgradient_downloader.py --save-csv`: Downloads data and saves it to a CSV file in the `data` directory.
    - `uv run python airgradient_downloader.py --to-motherduck`: Downloads data and uploads it to MotherDuck.
    - `uv run python airgradient_downloader.py --save-csv --to-motherduck`: Downloads data, saves it to a CSV, and uploads it to MotherDuck.

## Data Storage

### MotherDuck

- **Database:** Data is stored in a MotherDuck database.
- **Environment Variables:**
    - `MOTHERDUCK_TOKEN`: MotherDuck service token.
    - `MOTHERDUCK_DB_NAME`: The name of the database in MotherDuck.
    - `MOTHERDUCK_TABLE_NAME`: The name of the table to store the data.
- **Schema:** The table `airgradient_measures` has a composite primary key of `(locationId, timestamp, serialno)`.
- **Scripts:**
    - `airgradient_downloader.py`: Can be used to upload data to MotherDuck using the `--to-motherduck` flag.
    - `motherduck_updater.py`: A script to bulk-upload historical data from CSV files in the `data` directory to MotherDuck.

## Development

- **Language:** Python
- **Dependencies:** Dependencies are listed in the `pyproject.toml` file.
- **Dependency Management:** `uv` is used for dependency management.
- **Running Scripts:** Scripts should be run using `uv run`. For example, to run the data downloader script, use the command `uv run python airgradient_downloader.py`.

## CI/CD

- **GitHub Actions:** A GitHub Actions workflow in `.github/workflows/airgradient-data-download.yml` runs weekly to download the latest data, save it as a CSV, and upload it to MotherDuck.
- **Secrets:** The workflow requires the following secrets to be set in the repository:
    - `AIRGRADIENT_TOKEN`
    - `AIRGRADIENT_LOCATION_ID`
    - `MOTHERDUCK_TOKEN`
    - `MOTHERDUCK_DB_NAME`
    - `MOTHERDUCK_TABLE_NAME`