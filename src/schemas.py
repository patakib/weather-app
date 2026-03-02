"""Validating raw JSON files and loading them into Parquet"""

import polars as pl
import pandera.polars as pa


HOURLY_RAW_SCHEMA = pa.DataFrameSchema(
    {
        "time": pa.Column(
            pl.Utf8, pa.Check.str_matches(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}$")
        ),
        "temperature_2m": pa.Column(
            pl.Float64, pa.Check.in_range(-50, 50), nullable=True
        ),
        "precipitation": pa.Column(
            pl.Float64, pa.Check.in_range(0, 500), nullable=True
        ),
        "precipitation_probability": pa.Column(
            pl.Int64, pa.Check.in_range(0, 100), nullable=True
        ),
        "cloud_cover": pa.Column(pl.Int64, pa.Check.in_range(0, 100), nullable=True),
        "weather_code": pa.Column(pl.Int64),
        "wind_speed_10m": pa.Column(
            pl.Float64, pa.Check.in_range(0, 150), nullable=True
        ),
        "wind_direction_10m": pa.Column(
            pl.Int64, pa.Check.in_range(0, 360), nullable=True
        ),
    },
    coerce=True,
)

HOURLY_FINAL_SCHEMA = pa.DataFrameSchema(
    {
        "time": pa.Column(pl.Datetime("us"), nullable=False),
        "temperature_2m": pa.Column(
            pl.Float64, pa.Check.in_range(-50, 50), nullable=True
        ),
        "precipitation": pa.Column(
            pl.Float64, pa.Check.in_range(0, 500), nullable=True
        ),
        "precipitation_probability": pa.Column(
            pl.Int64, pa.Check.in_range(0, 100), nullable=True
        ),
        "cloud_cover": pa.Column(pl.Int64, pa.Check.in_range(0, 100), nullable=True),
        "weather_code": pa.Column(pl.Int64),
        "wind_speed_10m": pa.Column(
            pl.Float64, pa.Check.in_range(0, 150), nullable=True
        ),
        "wind_direction_10m": pa.Column(
            pl.Int64, pa.Check.in_range(0, 360), nullable=True
        ),
    },
    coerce=True,
)

DAILY_RAW_SCHEMA = pa.DataFrameSchema(
    {
        "time": pa.Column(
            pl.Utf8, pa.Check.str_matches(r"^\d{4}-\d{2}-\d{2}$"), nullable=False
        ),
        "temperature_2m_max": pa.Column(
            pl.Float64, pa.Check.in_range(-50, 50), nullable=True
        ),
        "temperature_2m_min": pa.Column(
            pl.Float64, pa.Check.in_range(-50, 50), nullable=True
        ),
        "sunrise": pa.Column(
            pl.Utf8, pa.Check.str_matches(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}$")
        ),
        "sunset": pa.Column(
            pl.Utf8, pa.Check.str_matches(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}$")
        ),
        "daylight_duration": pa.Column(
            pl.Float64, pa.Check.in_range(0, 86400), nullable=True
        ),
        "sunshine_duration": pa.Column(
            pl.Float64, pa.Check.in_range(0, 86400), nullable=True
        ),
        "uv_index_max": pa.Column(pl.Float64, pa.Check.in_range(0, 11), nullable=True),
        "precipitation_sum": pa.Column(
            pl.Float64, pa.Check.in_range(0, 500), nullable=True
        ),
        "precipitation_hours": pa.Column(
            pl.Float64, pa.Check.in_range(0, 24), nullable=True
        ),
        "snowfall_sum": pa.Column(pl.Float64, pa.Check.in_range(0, 500), nullable=True),
        "precipitation_probability_max": pa.Column(
            pl.Int64, pa.Check.in_range(0, 100), nullable=True
        ),
        "wind_speed_10m_max": pa.Column(
            pl.Float64, pa.Check.in_range(0, 150), nullable=True
        ),
        "wind_direction_10m_dominant": pa.Column(
            pl.Int64, pa.Check.in_range(0, 360), nullable=True
        ),
        "wind_gusts_10m_max": pa.Column(
            pl.Float64, pa.Check.in_range(0, 150), nullable=True
        ),
    },
    coerce=True,
)

DAILY_FINAL_SCHEMA = pa.DataFrameSchema(
    {
        "time": pa.Column(pl.Date),
        "temperature_2m_max": pa.Column(
            pl.Float64, pa.Check.in_range(-50, 50), nullable=True
        ),
        "temperature_2m_min": pa.Column(
            pl.Float64, pa.Check.in_range(-50, 50), nullable=True
        ),
        "sunrise": pa.Column(pl.Datetime("us"), nullable=True),
        "sunset": pa.Column(pl.Datetime("us"), nullable=True),
        "daylight_duration": pa.Column(
            pl.Float64, pa.Check.in_range(0, 86400), nullable=True
        ),
        "sunshine_duration": pa.Column(
            pl.Float64, pa.Check.in_range(0, 86400), nullable=True
        ),
        "uv_index_max": pa.Column(pl.Float64, pa.Check.in_range(0, 11), nullable=True),
        "precipitation_sum": pa.Column(
            pl.Float64, pa.Check.in_range(0, 500), nullable=True
        ),
        "precipitation_hours": pa.Column(
            pl.Float64, pa.Check.in_range(0, 24), nullable=True
        ),
        "snowfall_sum": pa.Column(pl.Float64, pa.Check.in_range(0, 500), nullable=True),
        "precipitation_probability_max": pa.Column(
            pl.Int64, pa.Check.in_range(0, 100), nullable=True
        ),
        "wind_speed_10m_max": pa.Column(
            pl.Float64, pa.Check.in_range(0, 150), nullable=True
        ),
        "wind_direction_10m_dominant": pa.Column(
            pl.Int64, pa.Check.in_range(0, 360), nullable=True
        ),
        "wind_gusts_10m_max": pa.Column(
            pl.Float64, pa.Check.in_range(0, 150), nullable=True
        ),
    },
    coerce=True,
)
