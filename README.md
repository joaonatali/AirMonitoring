# Air Monitoring Data Collection and Analysis

This project provides a set of tools to download and analyze air quality data from a personal AirGradient sensor. It can store data locally in CSV files and/or upload it to a MotherDuck database.

## Setup

1.  **Clone the repository and install dependencies:**
    ```bash
    git clone <repository-url>
    cd AirMonitoring
    uv sync
    ```

2.  **Configure environment variables:**
    This project uses a `.env` file to manage secrets and configuration. You can use `uv run dotenv` to create and manage it.

    *   **Required:** Set your AirGradient API token and location ID.
        ```bash
        uv run dotenv set AIRGRADIENT_TOKEN "your_airgradient_token"
        uv run dotenv set AIRGRADIENT_LOCATION_ID "your_location_id"
        ```

    *   **Optional:** To use MotherDuck for data storage, set your MotherDuck token and desired database/table names.
        ```bash
        uv run dotenv set MOTHERDUCK_TOKEN "your_motherduck_token"
        uv run dotenv set MOTHERDUCK_DB_NAME "my_air_quality_db"
        uv run dotenv set MOTHERDUCK_TABLE_NAME "airgradient_measures"
        ```

## Usage

### Data Download

The `airgradient_downloader.py` script is the primary tool for fetching new data from the AirGradient API.

*   **Download and print to console:**
    ```bash
    uv run python airgradient_downloader.py
    ```

*   **Download and save to a CSV file:**
    (The file will be saved in the `data/` directory)
    ```bash
    uv run python airgradient_downloader.py --save-csv
    ```

*   **Download and upload to MotherDuck:**
    (Requires MotherDuck environment variables to be set)
    ```bash
    uv run python airgradient_downloader.py --to-motherduck
    ```

*   **Download, save to CSV, and upload to MotherDuck:**
    ```bash
    uv run python airgradient_downloader.py --save-csv --to-motherduck
    ```

### Historical Data Upload

The `motherduck_updater.py` script can be used to bulk-upload historical data from existing CSV files in the `data` directory to your MotherDuck database.

```bash
uv run python motherduck_updater.py
```

### Using `just`

Install `just` with `brew install just`

- `just -l`


## CI/CD

This project includes a GitHub Actions workflow defined in `.github/workflows/airgradient-data-download.yml` that runs weekly.

This workflow automatically:
1.  Downloads the latest data from the AirGradient API.
2.  Saves the data to a new CSV file.
3.  Uploads the data to the specified MotherDuck database.

For the workflow to run successfully, you must configure the following secrets in your GitHub repository settings:
*   `AIRGRADIENT_TOKEN`
*   `AIRGRADIENT_LOCATION_ID`
*   `MOTHERDUCK_TOKEN`
*   `MOTHERDUCK_DB_NAME`
*   `MOTHERDUCK_TABLE_NAME`