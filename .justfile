
@_:
    just --list

set dotenv-load

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

# Backup MotherDuck table to R2 (uses .env for R2_BUCKET/R2_PREFIX)
r2backup:
    uv run --only-group ci python parquet_sync.py backup \
        --parquet-root "s3://{{ env_var('R2_BUCKET') }}{{ if env_var_or_default('R2_PREFIX', '') == '' { '' } else { '/' + env_var('R2_PREFIX') } }}" \
        --retain 4

# Validate latest R2 backup (uses .env for R2_BUCKET/R2_PREFIX)
r2validate:
    uv run --only-group ci python parquet_sync.py validate \
        --parquet-root "s3://{{ env_var('R2_BUCKET') }}{{ if env_var_or_default('R2_PREFIX', '') == '' { '' } else { '/' + env_var('R2_PREFIX') } }}" \
        --min-new-rows 2000
