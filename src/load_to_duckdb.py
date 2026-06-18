from datetime import datetime
import duckdb
import polars as pl
from pathlib import Path


DUCKDB_PATH = "data/warehouse/weather_dwh.duckdb"
PARQUET_FOLDER = Path("data/validated")


def init_duckdb(db_path: str, example_files: dict):
    """
    Initialize DuckDB with empty daily and hourly tables and a metadata table.

    :param db_path: Path to DuckDB database
    :param example_files: Dict with {'daily_data': path, 'hourly_data': path}
    """

    # determine if path exists for duck db database, if not create it
    db_file = Path(db_path)
    db_file.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(db_path)

    # Load GIS extension
    con.execute("INSTALL spatial;")
    con.execute("LOAD spatial;")

    for table_name, file_path in example_files.items():
        df_sample = pl.read_parquet(file_path, n_rows=1)
        columns = ", ".join(
            [
                f'"{c}" {pl_to_duckdb_type(df_sample[c].dtype)}'
                for c in df_sample.columns
            ]
        )
        con.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {columns},
            geom GEOMETRY
        );
        """)

    # Metadata table to track loaded files
    con.execute("""
    CREATE TABLE IF NOT EXISTS _loaded_files (
        table_name VARCHAR,
        file_name VARCHAR,
        load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (table_name, file_name)
    );
    """)

    return con


def pl_to_duckdb_type(dtype):
    """Map Polars dtype to DuckDB"""
    if dtype in [pl.Int8, pl.Int16, pl.Int32, pl.Int64]:
        return "BIGINT"
    elif dtype in [pl.Float32, pl.Float64]:
        return "DOUBLE"
    elif dtype == pl.Boolean:
        return "BOOLEAN"
    elif dtype == pl.Datetime:
        return "TIMESTAMP"
    else:
        return "VARCHAR"


def incremental_load(con, data_folder: Path, lat_col="latitude", lon_col="longitude"):
    # Already loaded files
    loaded_files = set(
        r[0] for r in con.execute("SELECT file_name FROM _loaded_files").fetchall()
    )

    for file_path in sorted(data_folder.glob("*.parquet")):
        fname = file_path.name
        if fname in loaded_files:
            continue

        # Determine table
        if fname.startswith("daily_data_"):
            table_name = "daily_data"
        elif fname.startswith("hourly_data_"):
            table_name = "hourly_data"
        else:
            print(f"Skipping unknown file: {fname}")
            continue

        # Insert directly from Parquet
        con.execute(f"""
        INSERT INTO {table_name}
        SELECT *, ST_Point("{lon_col}", "{lat_col}") AS geom
        FROM read_parquet('{file_path}')
        """)

        # Mark as loaded
        con.execute(
            "INSERT INTO _loaded_files (table_name, file_name) VALUES (?, ?)",
            [table_name, fname],
        )
        print(f"Loaded {fname} into {table_name}")

def determine_if_validated_data_exists():
    actual_daily_data_file = f"data/validated/daily_data_{datetime.now().strftime('%Y-%m-%d')}.parquet"
    actual_hourly_data_file = f"data/validated/hourly_data_{datetime.now().strftime('%Y-%m-%d')}.parquet"
    actual_daily_obj = Path(actual_daily_data_file)
    actual_hourly_obj = Path(actual_hourly_data_file)
    
    for path in [actual_daily_obj, actual_hourly_obj]:
        if path.exists():
            print(f"Current validated data file exist at {str(path)}.")
        else:
            print(f"The path does not exist at: {str(path)}.")
            print("Load to DuckDB cannot be completed.")
            raise FileNotFoundError
    return [actual_daily_data_file, actual_hourly_data_file]

def main():
    try:
        current_daily_data_file, current_hourly_data_file = determine_if_validated_data_exists()
        data = {
                "daily_data": current_daily_data_file,
                "hourly_data": current_hourly_data_file
        }
    except Exception as e:
        print(f"Data pipeline aborted due to error: {e}")
        raise
    con = init_duckdb(DUCKDB_PATH, data)
    # Incrementally load all new files in folder
    incremental_load(con, PARQUET_FOLDER)

if __name__ == "__main__":
    main()

