import logging
import get_raw_data
import validate_raw_data
import load_to_duckdb

logger = logging.getLogger(__name__)

def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - [WEATHER DATA PIPELINE] - %(name)s - %(message)s"
    )
    try:
        current_step = "Raw data download"
        logger.info("Running raw data download...")
        get_raw_data.main()
        logger.info("Raw data has been successfully downloaded.")

        current_step = "Data validation"
        logger.info("Running raw data validation...")
        validate_raw_data.main()
        logger.info("Data validation has been completed.")

        current_step = "Load to DuckDB"
        logger.info("Loading validated data into DuckDB...")
        load_to_duckdb.main()
        logger.info("Data load to DuckDB has been completed.")

    except Exception as e:
        logger.error(f"❌ Critical Failure during step: [{current_step}]")
        logger.exception(e)
        raise e

if __name__ == '__main__':
    main()

