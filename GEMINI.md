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
- **Script:** The `airgradient_downloader.py` script is responsible for fetching the data.

## Development

- **Language:** Python
- **Dependencies:** Dependencies are listed in the `pyproject.toml` file.
- **Dependency Management:** `uv` is used for dependency management.
- **Running Scripts:** Scripts should be run using `uv run`. For example, to run the data downloader script, use the command `uv run python airgradient_downloader.py`.
