
@_:
    just --list

# Download data from API but do not save it
download:
    uv run --only-group ci python airgradient_downloader.py

# Download data from API and save CSV
save_csv:
    uv run --only-group ci python airgradient_downloader.py --save-csv

# Download data from API and save to MotherDuck
save_md:
    uv run --only-group ci python airgradient_downloader.py --to-motherduck

# Download data from API and save to both CSV and MotherDuck
save_both:
    uv run --only-group ci python airgradient_downloader.py --save-csv --to-motherduck

# Upload all local data to MotherDuck
upload_local:
    uv run --only-group ci python motherduck_updater.py

# Update dependencies and DuckDB extensions
update:
    uv sync -U --group ci
