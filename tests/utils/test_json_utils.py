import pytest
from datetime import datetime, timezone
from sDownload.utils import json_dumps, parse_json_date


def test_json_dumps_date():
    dt = datetime(2026, 2, 21, 10, 0, 0, tzinfo=timezone.utc)
    data = {"date": dt}
    json_str = json_dumps(data)
    assert '"date": "2026-02-21T10:00:00Z"' in json_str


def test_json_dumps_date_naive_defaults_to_utc():
    # Naive datetime should be treated as UTC in our serializer
    dt = datetime(2026, 2, 21, 10, 0, 0)
    data = {"date": dt}
    json_str = json_dumps(data)
    assert '"date": "2026-02-21T10:00:00Z"' in json_str


def test_parse_json_date():
    date_str = "2026-02-21T10:00:00Z"
    dt = parse_json_date(date_str)
    assert dt.year == 2026
    assert dt.month == 2
    assert dt.day == 21
    assert dt.hour == 10
    assert dt.minute == 0
    assert dt.second == 0
    assert dt.tzinfo == timezone.utc


def test_parse_json_date_iso_with_tz():
    date_str = "2026-02-21T10:00:00+00:00"
    dt = parse_json_date(date_str)
    assert dt.tzinfo == timezone.utc


def test_json_dumps_unsupported_type():
    class Unknown:
        pass

    with pytest.raises(TypeError, match="not serializable"):
        json_dumps({"u": Unknown()})
