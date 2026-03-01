"""Utility functions for the data pipeline"""

from abc import ABC, abstractmethod
from pathlib import Path
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


class UrlBuilder(ABC):
    """details of API url and methods to dynamically create it"""

    def __init__(
        self,
        base_url: str = "https://api.open-meteo.com",
        api_version: str = "v1",
        daily_params: str = "temperature_2m_max,temperature_2m_min,sunrise,sunset,daylight_duration,sunshine_duration,uv_index_max,precipitation_sum,precipitation_hours,snowfall_sum,precipitation_probability_max,wind_speed_10m_max,wind_direction_10m_dominant,wind_gusts_10m_max",
        hourly_params: str = "temperature_2m,precipitation,precipitation_probability,cloud_cover,weather_code,wind_speed_10m,wind_direction_10m",
        locations: list[Location] = [],
    ):
        self.base_url = base_url
        self.api_version = api_version
        self.daily_parameters = daily_params
        self.hourly_parameters = hourly_params
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

    @abstractmethod
    def build_url(self) -> str:
        pass


class ForecastUrlBuilder(UrlBuilder):
    """details of API url and methods to dynamically create it"""

    def __init__(self, path: str = "forecast", forecast_days: int = 16, **kwargs):
        super().__init__(**kwargs)
        self.path = path
        self.forecast_days = forecast_days

    def build_url(self):
        return f"{self.base_url}/{self.api_version}/forecast?{self.latitude_string}&{self.longitude_string}&daily={self.daily_parameters}&hourly={self.hourly_parameters}&forecast_days={self.forecast_days}&timezone=auto"
