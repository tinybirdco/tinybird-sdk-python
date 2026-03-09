from __future__ import annotations

import pytest

from tinybird_sdk import Tinybird, define_datasource, t


def test_tinybird_class_is_public() -> None:
    client = Tinybird({"datasources": {}, "pipes": {}})
    assert isinstance(client, Tinybird)


def test_datasource_name_does_not_overwrite_internal_client_state() -> None:
    events = define_datasource("events", {"schema": {"id": t.string()}})
    client = Tinybird({"datasources": {"_client": events}, "pipes": {}})

    assert getattr(client, "_client") is not None
    with pytest.raises(ValueError, match="Client not initialized"):
        _ = client.client


def test_datasource_name_does_not_overwrite_internal_options_state() -> None:
    events = define_datasource("events", {"schema": {"id": t.string()}})
    client = Tinybird({"datasources": {"_options": events}, "pipes": {}})

    assert getattr(client, "_options") is not None
    with pytest.raises(ValueError, match="Client not initialized"):
        _ = client.client
