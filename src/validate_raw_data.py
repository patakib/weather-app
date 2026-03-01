"""Validating raw JSON files and loading them into Parquet"""

from pathlib import Path
from datetime import datetime
import json
import polars as pl


HOURLY_RAW_SCHEMA = {
    "time": pl.Utf8(),
    "temperature_2m": pl.Float64,
    "precipitation": pl.Float64,
    "precipitation_probability": pl.Int64,
    "cloud_cover": pl.Int64,
    "weather_code": pl.Int64,
    "wind_speed_10m": pl.Float64,
    "wind_direction_10m": pl.Int64,
}
HOURLY_FINAL_SCHEMA = {
    "time": pl.Datetime("us"),
    "temperature_2m": pl.Float64,
    "precipitation": pl.Float64,
    "precipitation_probability": pl.Int64,
    "cloud_cover": pl.Int64,
    "weather_code": pl.Int64,
    "wind_speed_10m": pl.Float64,
    "wind_direction_10m": pl.Int64,
}

DAILY_RAW_SCHEMA = {
    "time": pl.Utf8(),
    "temperature_2m_max": pl.Float64,
    "temperature_2m_min": pl.Float64,
    "sunrise": pl.Utf8(),
    "sunset": pl.Utf8(),
    "daylight_duration": pl.Float64,
    "sunshine_duration": pl.Float64,
    "uv_index_max": pl.Float64,
    "precipitation_sum": pl.Float64,
    "precipitation_hours": pl.Float64,
    "snowfall_sum": pl.Float64,
    "precipitation_probability_max": pl.Int64,
    "wind_speed_10m_max": pl.Float64,
    "wind_direction_10m_dominant": pl.Int64,
    "wind_gusts_10m_max": pl.Float64,
}
DAILY_FINAL_SCHEMA = {
    "time": pl.Date,
    "temperature_2m_max": pl.Float64,
    "temperature_2m_min": pl.Float64,
    "sunrise": pl.Datetime("us"),
    "sunset": pl.Datetime("us"),
    "daylight_duration": pl.Float64,
    "sunshine_duration": pl.Float64,
    "uv_index_max": pl.Float64,
    "precipitation_sum": pl.Float64,
    "precipitation_hours": pl.Float64,
    "snowfall_sum": pl.Float64,
    "precipitation_probability_max": pl.Int64,
    "wind_speed_10m_max": pl.Float64,
    "wind_direction_10m_dominant": pl.Int64,
    "wind_gusts_10m_max": pl.Float64,
}


def read_json_data(filename: str | Path) -> list[dict]:
    """Read JSON data from a file and return it as a dictionary."""
    data: list[dict] = []
    try:
        data = json.load(open(filename, "r"))
        return data
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON from file {filename}: {e}")
        raise


def parse_data_dict_strict_to_polars(
    data: dict,
    raw_schema: pl.Schema,
    final_schema: pl.Schema,
) -> pl.DataFrame:
    """Load hourly or daily data from a dictionary into a Polars DataFrame and enforce schema"""
    try:
        df = pl.DataFrame(data, schema=raw_schema, strict=True)
    except Exception as e:
        raise ValueError(f"Raw schema validation failed: {e}")

    try:
        df = df.cast(final_schema, strict=True)
    except Exception as e:
        raise ValueError(f"Final schema validation failed: {e}")

    return df


def create_polars_dataframes_from_json(
    json_file: str | Path,
    hourly_raw_schema: pl.Schema = HOURLY_RAW_SCHEMA,
    hourly_final_schema: pl.Schema = HOURLY_FINAL_SCHEMA,
    daily_raw_schema: pl.Schema = DAILY_RAW_SCHEMA,
    daily_final_schema: pl.Schema = DAILY_FINAL_SCHEMA,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Load JSON data from a file, validate it, concat elements and return as Polars Dfs"""
    data = read_json_data(json_file)
    hourly_tables = []
    daily_tables = []
    for i in range(len(data)):
        # hourly data
        hourly_pldf = parse_data_dict_strict_to_polars(
            data[i]["hourly"],
            hourly_raw_schema,
            hourly_final_schema,
        )
        # additional information to the DataFrame
        hourly_pldf = hourly_pldf.with_columns(
            pl.lit(data[i]["latitude"]).alias("latitude"),
            pl.lit(data[i]["longitude"]).alias("longitude"),
            pl.lit(data[i]["city"]).alias("city"),
            pl.lit(datetime.now()).alias("log_time"),
        )

        # append table to list
        hourly_tables.append(hourly_pldf)

        # daily data
        daily_pldf = parse_data_dict_strict_to_polars(
            data[i]["daily"],
            daily_raw_schema,
            daily_final_schema,
        )
        # additional information to the DataFrame
        daily_pldf = daily_pldf.with_columns(
            pl.lit(data[i]["latitude"]).alias("latitude"),
            pl.lit(data[i]["longitude"]).alias("longitude"),
            pl.lit(data[i]["city"]).alias("city"),
            pl.lit(datetime.now()).alias("log_time"),
        )

        # append table to list
        daily_tables.append(daily_pldf)

    # concatenate all tables into one
    full_hourly_table = pl.concat(hourly_tables, how="vertical", rechunk=True)
    full_daily_table = pl.concat(daily_tables, how="vertical", rechunk=True)

    return full_hourly_table, full_daily_table


def save_polars_dataframes_to_parquet(
    hourly_table: pl.DataFrame,
    daily_table: pl.DataFrame,
    parquet_folder: str | Path,
) -> None:
    current_date = datetime.now().strftime("%Y-%m-%d")

    # make sure folder exists
    Path(parquet_folder).mkdir(parents=True, exist_ok=True)

    # write hourly parquet file
    hourly_table.write_parquet(
        Path(parquet_folder) / f"hourly_data_{current_date}.parquet",
    )
    # write daily parquet file
    daily_table.write_parquet(
        Path(parquet_folder) / f"daily_data_{current_date}.parquet",
    )


def sanity_check_parquet_files(parquet_folder: str | Path) -> None:
    """Perform a sanity check on the generated Parquet files."""
    current_date = datetime.now().strftime("%Y-%m-%d")
    hourly_file = Path(parquet_folder) / f"hourly_data_{current_date}.parquet"
    daily_file = Path(parquet_folder) / f"daily_data_{current_date}.parquet"

    if not hourly_file:
        print("No hourly Parquet files found for today.")

    if not daily_file:
        print("No daily Parquet files found for today.")

    try:
        df = pl.read_parquet(hourly_file)
        print(f"Successfully read hourly Parquet file: {hourly_file}")
    except Exception as e:
        print(f"Error reading hourly Parquet file {hourly_file}: {e}")
    try:
        print(f"Schema of hourly Parquet file {hourly_file}: {df.schema}")
        print(f"{df.shape[0]} rows, {df.shape[1]} columns")
        print(f"{df.head()}")
    except Exception as e:
        print(f"Error inspecting hourly Parquet file {hourly_file}: {e}")

    try:
        df = pl.read_parquet(daily_file)
        print(f"Successfully read daily Parquet file: {daily_file}")
    except Exception as e:
        print(f"Error reading daily Parquet file {daily_file}: {e}")
    try:
        print(f"Schema of daily Parquet file {daily_file}: {df.schema}")
        print(f"{df.shape[0]} rows, {df.shape[1]} columns")
        print(f"{df.head()}")
    except Exception as e:
        print(f"Error inspecting daily Parquet file {daily_file}: {e}")
    print("Sanity check completed.")


def validate_and_load_json_to_parquet(
    json_file: str | Path,
    parquet_folder: str | Path,
) -> None:
    """Validate raw JSON data and load it into Parquet files."""
    hourly_table, daily_table = create_polars_dataframes_from_json(json_file)
    save_polars_dataframes_to_parquet(hourly_table, daily_table, parquet_folder)
    sanity_check_parquet_files(parquet_folder)


if __name__ == "__main__":
    validate_and_load_json_to_parquet("data/raw/raw_2026-02-21.json", "data/validated")
