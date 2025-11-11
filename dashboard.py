# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "altair==5.5.0",
#     "duckdb==1.4.1",
#     "polars[pyarrow]==1.34.0",
#     "pyarrow==21.0.0",
#     "python-dotenv==1.1.1",
#     "python-lsp-ruff==2.3.0",
#     "python-lsp-server==1.13.1",
#     "sqlglot==27.28.1",
#     "vegafusion==2.0.3",
#     "vl-convert-python==1.8.0",
#     "websockets==15.0.1",
# ]
# ///

import marimo

__generated_with = "0.16.0"
app = marimo.App(width="medium")

with app.setup(hide_code=True):
    # Initialization code that runs before all other cells
    import marimo as mo
    import polars as pl
    import altair as alt
    import duckdb
    import os
    from dotenv import load_dotenv

    alt.data_transformers.enable("vegafusion")
    load_dotenv(".env")


@app.cell(hide_code=True)
def _():
    mo.md(r"""# Air Monitoring Dashboard""")
    return


@app.cell
def _():
    con = duckdb.connect(
        f"md:{os.environ['MOTHERDUCK_DB_NAME']}?motherduck_token={os.environ['MOTHERDUCK_TOKEN']}"
    )
    con.sql("SHOW TABLES;")
    return (con,)


@app.cell
def _(airgradient_measures, con):
    _df = mo.sql(
        f"""
        SELECT
            locationid, locationname, serialno, updated_at,
        	COUNT(*) AS nrows
        FROM airgradient_measures
        GROUP BY ALL
        """,
        engine=con
    )
    return


@app.cell
def _(airgradient_measures, con):
    _ = mo.sql(
        f"""
        SELECT COUNT(*) FROM airgradient_measures
        """,
        engine=con
    )
    return


@app.cell
def _(airgradient_measures, con):
    data = mo.sql(
        f"""
        SELECT * FROM airgradient_measures
        """,
        engine=con
    )
    return (data,)


@app.cell(hide_code=True)
def _(data):
    metrics = [
        m
        for m in data.columns
        if (
            m.startswith("pm")
            | m.startswith("rco2")
            | m.startswith("atmp")
            | m.startswith("tvoc")
            | m.startswith("rhum")
        )
    ]
    metric_selector = mo.ui.multiselect(metrics, value=metrics[0:1])

    date_range = mo.ui.date_range(start="2025-06-01", stop="2025-12-31")

    opacity_selector = mo.ui.slider(0, 1, 0.1, 0.5)
    return date_range, metric_selector, metrics, opacity_selector


@app.cell(hide_code=True)
def _(data, date_range, metric_selector, metrics, opacity_selector):
    metrics_selected = (
        metric_selector.value if len(metric_selector.value) >= 1 else [metrics[0]]
    )
    ts_chart = (
        alt.Chart(
            data.filter(
                pl.col("timestamp") >= date_range.value[0],
                pl.col("timestamp") <= date_range.value[1],
            )
            .select(["timestamp", *metrics_selected])
            .unpivot(
                index="timestamp",
                on=metrics_selected,
                variable_name="metric",
                value_name="value",
            )
            .sort(["metric", "timestamp"])
            .group_by_dynamic(
                index_column="timestamp",  # 'y', 'mo', 'q', 'w', 'd', 'h', 'm', 's', 'ms', 'us', 'ns'
                every="1h"
                if (date_range.value[1] - date_range.value[0]).days < 30
                else (
                    "1d"
                    if (date_range.value[1] - date_range.value[0]).days < 120
                    else "1w"
                ),
                group_by="metric",
            )
            .agg(pl.col("value").mean()),
            width=600,
        )
        .mark_line(opacity=opacity_selector.value)
        .encode(
            x="timestamp",
            y="value",
            color="metric",
            tooltip="metric",
        )
        .interactive()
    )

    mo.md(
        f"""
        ## Time series of measurement

        Date Range: {date_range}

        Metric: {metric_selector}

        Opacity: {opacity_selector}

        {mo.as_html(ts_chart)}
        """
    )
    return


@app.cell
def _(data):
    import numpy as np

    # Calculate outliers using IQR method on rco2
    q1 = data["rco2"].quantile(0.15)
    q3 = data["rco2"].quantile(0.85)
    iqr = q3 - q1
    lower_bound = q1 - 1.5 * iqr
    upper_bound = q3 + 1.5 * iqr

    rco2_outliers = data.filter(
        (pl.col("rco2") < lower_bound) | (pl.col("rco2") > upper_bound)
    )

    rco2_outliers
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
