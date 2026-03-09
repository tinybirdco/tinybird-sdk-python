from tinybird_sdk.api.dashboard import (
    get_branch_dashboard_url,
    get_dashboard_url,
    get_local_dashboard_url,
    parse_api_url,
)


def test_parse_api_url() -> None:
    region = parse_api_url("https://api.tinybird.co")
    assert region is not None
    assert region.provider == "gcp"
    assert region.region == "europe-west3"


def test_dashboard_url_helpers() -> None:
    assert (
        get_dashboard_url("https://api.tinybird.co", "my_workspace")
        == "https://cloud.tinybird.co/gcp/europe-west3/my_workspace"
    )
    assert (
        get_branch_dashboard_url("https://api.tinybird.co", "my_workspace", "feature_x")
        == "https://cloud.tinybird.co/gcp/europe-west3/my_workspace~feature_x"
    )
    assert (
        get_local_dashboard_url("my_local_workspace")
        == "https://cloud.tinybird.co/local/7181/my_local_workspace"
    )
