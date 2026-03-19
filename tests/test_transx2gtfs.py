import pytest

from txc2gtfs.data import get_path


@pytest.fixture
def test_data():
    return get_path("test_data_dir")


@pytest.fixture
def test_tfl_data():
    return get_path("test_tfl_format")


@pytest.fixture
def test_txc21_data():
    return get_path("test_txc21_format")


@pytest.fixture
def temp_output_filepath():
    import os
    import tempfile

    temp_dir = tempfile.gettempdir()
    temp_fp = os.path.join(temp_dir, "test_gtfs.zip")
    return temp_fp


def test_agency_urls():
    import requests

    from txc2gtfs.agency import get_agency_url

    operator_codes = [
        "OId_LUL",
        "OId_DLR",
        "OId_TRS",
        "OId_CCR",
        "OId_CV",
        "OId_WFF",
        "OId_TCL",
        "OId_EAL",
        #'OId_CRC'
    ]
    for code in operator_codes:
        url = get_agency_url(code)

        req = requests.get(url)
        assert req.status_code == 200, f"Web site '{url}' does not exist."


def test_converting_to_gtfs(test_data, temp_output_filepath):
    import os
    from zipfile import ZipFile

    import txc2gtfs

    # Do the conversion
    txc2gtfs.convert(test_data, temp_output_filepath)

    # Check that the zip-file was created
    assert os.path.isfile(temp_output_filepath)

    # Check the contents
    zf = ZipFile(temp_output_filepath)
    zip_contents = zf.namelist()

    required_files = [
        "stops.txt",
        "agency.txt",
        "stop_times.txt",
        "trips.txt",
        "calendar.txt",
        "routes.txt",
    ]
    for file in required_files:
        assert file in zip_contents
