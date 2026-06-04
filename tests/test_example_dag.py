from __future__ import annotations

from datetime import date, timedelta

from examples.builder_reports_dag import get_date_range


def test_date_range_multiple_days():
    result = get_date_range("2024-01-01", "2024-01-05")
    assert result == ["2024-01-05", "2024-01-04", "2024-01-03", "2024-01-02", "2024-01-01"]


def test_date_range_single_day():
    result = get_date_range("2024-01-01", "2024-01-01")
    assert result == ["2024-01-01"]


def test_date_range_default_30_days():
    yesterday = date.today() - timedelta(days=1)
    date_to = yesterday.isoformat()
    date_from = (yesterday - timedelta(days=29)).isoformat()
    result = get_date_range(date_from, date_to)
    assert len(result) == 30
    assert result[0] == date_to
    assert result[-1] == date_from
