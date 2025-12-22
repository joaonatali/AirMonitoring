# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "altair>=6.0.0",
#     "duckdb>=1.4.3",
#     "numpy>=2.3.5",
#     "openai>=2.14.0",
#     "polars[pyarrow]==1.36.1",
#     "pyarrow>=22.0.0",
#     "python-dotenv>=1.1.1",
#     "python-lsp-ruff>=2.3.0",
#     "python-lsp-server>=1.14.0",
#     "sqlglot>=28.5.0",
#     "vegafusion>=2.0.3",
#     "vl-convert-python>=1.8.0",
#     "websockets>=15.0.1",
# ]
# ///

import marimo

__generated_with = "0.18.4"
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
    mo.md(r"""
    # Air Monitoring Dashboard
    """)
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
            MIN(timestamp) AS min_ts,
            MAX(timestamp) AS max_ts,
            DATE_DIFF('day', min_ts, max_ts) AS span_days,
        	COUNT(*) AS nrows,
            SUM(datapoints) AS datapoints,
        FROM airgradient_measures
        GROUP BY ALL
        ORDER BY 1, 2, 3, 4 DESC
        """,
        engine=con,
    )
    return


@app.cell
def _(airgradient_measures, con):
    _ = mo.sql(
        f"""
        SELECT COUNT(*) FROM airgradient_measures
        """,
        engine=con,
    )
    return


@app.cell
def _(airgradient_measures, con):
    data = mo.sql(
        f"""
        SELECT * FROM airgradient_measures
        """,
        engine=con,
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

    updated_at_list = data["updated_at"].unique().sort(descending=True).to_list()
    upload_date_selector = mo.ui.dropdown(updated_at_list, value=updated_at_list[0])

    opacity_selector = mo.ui.slider(0, 1, 0.1, 0.5)
    return (
        date_range,
        metric_selector,
        metrics,
        opacity_selector,
        upload_date_selector,
    )


@app.cell(hide_code=True)
def _(data, metric_selector, metrics, opacity_selector, upload_date_selector):
    _metrics_selected = (
        metric_selector.value if len(metric_selector.value) >= 1 else [metrics[0]]
    )
    val_chart = (
        alt.Chart(
            data.filter(pl.col("updated_at") == upload_date_selector.value)
            .select(["timestamp", *_metrics_selected])
            .unpivot(
                index="timestamp",
                on=_metrics_selected,
                variable_name="metric",
                value_name="value",
            )
            .sort(["metric", "timestamp"]),
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
        ## Validation of download batches

        Upload_date: {upload_date_selector}

        Metric: {metric_selector}

        Opacity: {opacity_selector}

        {mo.as_html(val_chart)}
        """
    )
    return


@app.cell(hide_code=True)
def _(data, date_range, metric_selector, metrics, opacity_selector):
    _metrics_selected = (
        metric_selector.value if len(metric_selector.value) >= 1 else [metrics[0]]
    )
    ts_chart = (
        alt.Chart(
            data.filter(
                pl.col("timestamp") >= date_range.value[0],
                pl.col("timestamp") <= date_range.value[1],
            )
            .group_by(
                ["locationId", "locationName", "serialno", "timestamp"]
            )  # Note that there may be duplicated in the loading
            .agg([pl.col(m).mean().alias(m) for m in _metrics_selected])
            .select(["timestamp", *_metrics_selected])
            .unpivot(
                index="timestamp",
                on=_metrics_selected,
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
    # Calculate outliers using IQR method on rco2
    _q1 = data["rco2"].quantile(0.15)
    _q3 = data["rco2"].quantile(0.85)
    _iqr = _q3 - _q1
    _lower_bound = _q1 - 1.5 * _iqr
    _upper_bound = _q3 + 1.5 * _iqr

    _df_rco2 = (
        data.select(["timestamp", "rco2"])
        .with_columns(
            (
                (pl.col("rco2") < _lower_bound) | (pl.col("rco2") > _upper_bound)
            ).alias("is_outlier")
        )
        .sort("timestamp")
    )

    base = (
        alt.Chart(_df_rco2, title="rCO2 Time Series with Outliers Highlighted")
        .mark_line(color="#2c7fb8", opacity=0.7)
        .encode(
            x=alt.X("timestamp:T", title="Timestamp"),
            y=alt.Y("rco2:Q", title="rCO2"),
            tooltip=[
                alt.Tooltip("timestamp:T", title="Timestamp"),
                alt.Tooltip("rco2:Q", title="rCO2"),
            ],
        )
    )

    outlier_points = (
        alt.Chart(_df_rco2)
        .mark_point(color="red", size=60)
        .encode(
            x="timestamp:T",
            y="rco2:Q",
            tooltip=[
                alt.Tooltip("timestamp:T", title="Timestamp"),
                alt.Tooltip("rco2:Q", title="rCO2"),
                alt.Tooltip("is_outlier:N", title="Outlier"),
            ],
        )
        .transform_filter(alt.datum.is_outlier)
    )

    chart = (base + outlier_points).properties(width=700, height=350).interactive()

    chart
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
