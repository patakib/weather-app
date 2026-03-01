from pathlib import Path
from utils import load_config, HistoricalUrlBuilder, RawDataHandler


def main(destination_folder: Path | str = "data/raw/historical"):
    locations = load_config().locations
    raw_data_handler = RawDataHandler(
        locations=locations,
        url_builder_class=HistoricalUrlBuilder,
        destination_folder=destination_folder,
    )
    raw_data = raw_data_handler.fetch_raw_data()
    raw_data_enriched = raw_data_handler.enrich_raw_data(raw_data)
    raw_data_handler.save_raw_data(raw_data_enriched)


if __name__ == "__main__":
    main()
