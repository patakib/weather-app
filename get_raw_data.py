import requests
import json
from pathlib import Path
from datetime import datetime
import tomllib
from pydantic import BaseModel, Field, field_validator, model_validator


class Location(BaseModel):
    """Holds location data for a city"""

    city: str
    latitude: float
    longitude: float

    @field_validator("latitude")
    @classmethod
    def validate_latitude(cls, v: float) -> float:
        if not (-90 <= v <= 90):
            raise ValueError("Latitude must be between -90 and 90 degrees.")
        return v

    @field_validator("longitude")
    @classmethod
    def validate_longitude(cls, v: float) -> float:
        if not (-180 <= v <= 180):
            raise ValueError("Longitude must be between -180 and 180 degrees.")
        return v


class Config(BaseModel):
    """Holds configuration data"""

    locations: list[Location] = Field(..., min_length=1)

    @model_validator(mode="before")
    @classmethod
    def check_locations_exist(cls, values):
        if "locations" not in values or not values["locations"]:
            raise ValueError("At least one location must be provided.")
        return values


def load_config(filename: str | Path = "config.toml") -> Config:
    """Loads configuration file"""
    path = Path(filename)
    if not path.exists():
        raise FileNotFoundError(f"Configuration file '{filename}' not found.")
    with path.open("rb") as f:
        try:
            config_data = tomllib.load(f)
            return Config(**config_data)
        except tomllib.TOMLDecodeError as e:
            raise ValueError(f"Failed to parse TOML file: {e}")
        except Exception as e:
            raise ValueError(f"An error occurred while loading the configuration: {e}")


class UrlBuilder:
    """details of API url and methods to dynamically create it"""

    base_url = "https://api.open-meteo.com/v1/forecast?"
    daily_forecast_parameters = "temperature_2m_max,temperature_2m_min,sunrise,sunset,daylight_duration,sunshine_duration,uv_index_max,precipitation_sum,precipitation_hours,snowfall_sum,precipitation_probability_max,wind_speed_10m_max,wind_direction_10m_dominant,wind_gusts_10m_max"
    hourly_forecast_parameters = "temperature_2m,precipitation,precipitation_probability,cloud_cover,weather_code,wind_speed_10m,wind_direction_10m"
    forecast_days = 16

    def __init__(self, locations: list[Location]):
        self.latitude_string, self.longitude_string = (
            self._create_comma_separated_list_of_coordinates(locations)
        )

    def _create_comma_separated_list_of_coordinates(
        self, locations: list[Location]
    ) -> tuple[str, str]:
        latitude_string: str = "latitude="
        longitude_string: str = "longitude="
        counter = 0
        for location in locations:
            latitude_string = latitude_string + str(location.latitude)
            longitude_string = longitude_string + str(location.longitude)
            counter += 1
            if counter < len(locations):
                latitude_string = latitude_string + ","
                longitude_string = longitude_string + ","
        return latitude_string, longitude_string

    def build_url(self):
        return f"{self.base_url}{self.latitude_string}&{self.longitude_string}&daily={self.daily_forecast_parameters}&hourly={self.hourly_forecast_parameters}&forecast_days={self.forecast_days}"


class RawDataHandler:
    """Responsible for getting and sinking raw data as-is from API"""

    def __init__(self, locations: list[Location], destination_folder: Path | str):
        self.locations = locations
        self.url_builder = UrlBuilder(locations=self.locations)
        self.destination_folder = Path(destination_folder)
        self.destination_folder.mkdir(parents=True, exist_ok=True)

    def _generate_filename_with_date(self) -> str:
        current_date = datetime.now().strftime("%Y-%m-%d")
        return f"raw_{current_date}.json"

    def fetch_raw_data(self) -> dict:
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

    def save_raw_data(self, data: dict):
        try:
            filename = self._generate_filename_with_date()
            file_path = self.destination_folder / filename
            with open(file_path, "w") as f:
                json.dump(data, f, indent=4)
            print(f"Raw data saved successfully to {file_path}")
        except Exception as e:
            print(f"Failed to save raw JSON data: {e}")


def main():
    locations = load_config().locations
    raw_data_handler = RawDataHandler(
        locations=locations, destination_folder="data/raw"
    )
    raw_data = raw_data_handler.fetch_raw_data()
    raw_data_handler.save_raw_data(raw_data)


if __name__ == "__main__":
    main()
