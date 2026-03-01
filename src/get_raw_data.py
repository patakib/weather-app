import requests
import json
from pathlib import Path
from datetime import datetime
from utils import Location, load_config, ForecastUrlBuilder


class RawDataHandler:
    """Responsible for getting and sinking raw data as-is from API"""

    def __init__(self, locations: list[Location], destination_folder: Path | str):
        self.locations = locations
        self.url_builder = ForecastUrlBuilder(locations=self.locations)
        self.destination_folder = Path(destination_folder)
        self.destination_folder.mkdir(parents=True, exist_ok=True)

    def _generate_filename_with_date(self) -> str:
        current_date = datetime.now().strftime("%Y-%m-%d")
        return f"raw_{current_date}.json"

    def fetch_raw_data(self) -> list[dict]:
        url = self.url_builder.build_url()
        data = {}
        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
        except requests.exceptions.RequestException as e:
            print(f"HTTP Request failed: {e}")
        except json.JSONDecodeError as e:
            print(f"Failed to parse JSON: {e}")
        return data

    def enrich_raw_data(self, data: list[dict]) -> list[dict]:
        """Add city information to the raw data for better traceability"""
        for i in range(len(data)):
            data[i]["city"] = self.locations[i].city
        return data

    def save_raw_data(self, data: list):
        try:
            filename = self._generate_filename_with_date()
            file_path = self.destination_folder / filename
            with open(file_path, "w") as f:
                json.dump(data, f, indent=4)
            print(f"Raw data saved successfully to {file_path}")
        except Exception as e:
            print(f"Failed to save raw JSON data: {e}")


def main(destination_folder: Path | str = "data/raw"):
    locations = load_config().locations
    raw_data_handler = RawDataHandler(
        locations=locations, destination_folder=destination_folder
    )
    raw_data = raw_data_handler.fetch_raw_data()
    raw_data_enriched = raw_data_handler.enrich_raw_data(raw_data)
    raw_data_handler.save_raw_data(raw_data_enriched)


if __name__ == "__main__":
    main()
