"""Tests for get_raw_data.py"""

import pytest

from get_raw_data import Location
from pydantic import ValidationError


@pytest.mark.parametrize(
    "city, latitude, longitude, should_fail",
    [
        ("New York", 40.7128, -74.0060, False),
        ("Los Angeles", 34.0522, -118.2437, False),
        ("Chicago", 41.8781, -87.6298, False),
        ("Invalid Latitude", 100.0, 50.0, True),
        ("Invalid Longitude", 50.0, 200.0, True),
        ("Edge Case Latitude", -90.0, 0.0, False),
        ("Edge Case Longitude", 0.0, -180.0, False),
    ],
)
def test_valid_locations_parsed(city, latitude, longitude, should_fail):
    if should_fail:
        with pytest.raises(ValidationError):
            Location(city=city, latitude=latitude, longitude=longitude)
    else:
        location = Location(city=city, latitude=latitude, longitude=longitude)
        assert location.city == city
        assert location.latitude == latitude
        assert location.longitude == longitude
