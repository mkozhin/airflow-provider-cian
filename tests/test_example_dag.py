from __future__ import annotations

from datetime import date, timedelta
from unittest.mock import MagicMock

from examples.builder_reports_dag import _cleanup, get_date_range


def _make_ti(xcom_return):
    ti = MagicMock(name="ti")
    ti.xcom_pull.return_value = xcom_return
    return ti


class TestCleanup:
    """_cleanup under the new collect XCom contract (dict | None)."""

    def test_none_does_not_raise(self):
        # Fully empty period: xcom_pull returns None, not [].
        ti = _make_ti(None)
        assert _cleanup(ti) is None
        ti.xcom_pull.assert_called_once_with(task_ids="collect")

    def test_empty_list_does_not_raise(self):
        ti = _make_ti([])
        assert _cleanup(ti) is None

    def test_deletes_item_path_for_dict_items(self, tmp_path):
        file_a = tmp_path / "2024-01-01.json"
        file_b = tmp_path / "2024-01-02.json"
        file_a.write_text("[]")
        file_b.write_text("[]")

        items = [
            {"date": "2024-01-01", "path": str(file_a)},
            {"date": "2024-01-02", "path": str(file_b)},
        ]
        ti = _make_ti(items)
        _cleanup(ti)

        assert not file_a.exists()
        assert not file_b.exists()

    def test_missing_file_does_not_raise(self, tmp_path):
        missing = tmp_path / "gone.json"
        items = [{"date": "2024-01-01", "path": str(missing)}]
        ti = _make_ti(items)
        # File never existed — must not raise.
        assert _cleanup(ti) is None
        assert not missing.exists()


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
