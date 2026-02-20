"""Functional tests for get_raw_data"""

import os
from datetime import datetime
from pathlib import Path
import pytest

from get_raw_data import main


@pytest.fixture
def cleanup_weather_file():
    # SETUP: Define the file path
    destination_file = Path(
        "test_data/raw/raw_" + datetime.now().strftime("%Y-%m-%d") + ".json"
    )

    yield destination_file  # The test runs here

    # TEARDOWN: This runs AFTER the assertions, win or lose
    if destination_file.exists():
        destination_file.unlink()
        print(f"\nCleaned up {destination_file}")


def test_get_raw_data(cleanup_weather_file):
    # check if it run without failing and the file is created

    destination_file = cleanup_weather_file

    main(destination_folder="test_data/raw")

    assert os.path.isfile(destination_file), (
        f"Expected file {destination_file} to be created."
    )

    # check if file is not empty and contains valid JSON
    assert os.path.getsize(destination_file) > 0, (
        f"Expected file {destination_file} to not be empty."
    )
