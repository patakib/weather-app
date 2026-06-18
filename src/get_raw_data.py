import logging
from pathlib import Path
from utils import load_config, ForecastUrlBuilder, RawDataHandler


def main(destination_folder: Path | str = "data/raw"):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - [GET RAW DATA] - %(name)s - %(message)s"
    )
    locations = load_config().locations
    raw_data_handler = RawDataHandler(
        locations=locations,
        url_builder_class=ForecastUrlBuilder,
        destination_folder=destination_folder,
    )
    raw_data = raw_data_handler.fetch_raw_data()
    raw_data_enriched = raw_data_handler.enrich_raw_data(raw_data)
    raw_data_handler.save_raw_data(raw_data_enriched)


if __name__ == "__main__":
    main()
