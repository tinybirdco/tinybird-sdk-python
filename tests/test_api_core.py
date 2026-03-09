from tinybird_sdk.api.api import TinybirdApi


def test_api_constructor_validation() -> None:
    try:
        TinybirdApi({"base_url": "", "token": "x"})
        assert False, "expected ValueError"
    except ValueError as error:
        assert "base_url is required" in str(error)

    try:
        TinybirdApi({"base_url": "https://api.tinybird.co", "token": ""})
        assert False, "expected ValueError"
    except ValueError as error:
        assert "token is required" in str(error)


def test_ingest_batch_empty_returns_zero_counts() -> None:
    api = TinybirdApi({"base_url": "https://api.tinybird.co", "token": "p.test"})
    result = api.ingest_batch("events", [])
    assert result == {"successful_rows": 0, "quarantined_rows": 0}


def test_append_validation() -> None:
    api = TinybirdApi({"base_url": "https://api.tinybird.co", "token": "p.test"})

    try:
        api.append_datasource("events", {})
        assert False, "expected ValueError"
    except ValueError as error:
        assert "Either 'url' or 'file'" in str(error)

    try:
        api.append_datasource("events", {"url": "https://x", "file": "./x.csv"})
        assert False, "expected ValueError"
    except ValueError as error:
        assert "Only one of 'url' or 'file'" in str(error)
