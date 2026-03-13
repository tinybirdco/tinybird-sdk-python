from tinybird_sdk.api.fetcher import with_tinybird_from_param


def test_with_tinybird_from_param_sets_from_query_param() -> None:
    url = with_tinybird_from_param("https://api.tinybird.co/v1/workspace")
    assert "from=python-sdk" in url


def test_with_tinybird_from_param_preserves_existing_query() -> None:
    url = with_tinybird_from_param("https://api.tinybird.co/v1/build?foo=bar")
    assert "foo=bar" in url
    assert "from=python-sdk" in url
