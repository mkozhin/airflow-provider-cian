from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from airflow.exceptions import AirflowException
from airflow.models import Connection

from airflow_provider_cian.hooks.cian import CianHook


def _make_hook() -> CianHook:
    hook = CianHook(cian_conn_id="cian_test")
    conn = Connection(
        conn_id="cian_test",
        conn_type="http",
        host="https://public-api.cian.ru",
        password="test-token",
    )
    hook.get_connection = MagicMock(return_value=conn)
    return hook


def _mock_response(status_code: int, json_data: dict | None = None) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_data or {}
    resp.text = str(json_data)
    return resp


class TestGetBuilderReports:
    def test_success_returns_reports(self):
        hook = _make_hook()
        reports = [{"id": 1, "newbuildingId": 10}]
        response = _mock_response(200, {"result": {"reports": reports}})

        with patch("requests.get", return_value=response) as mock_get:
            result = hook.get_builder_reports("2024-01-01")

        assert result == reports
        mock_get.assert_called_once()
        call_kwargs = mock_get.call_args
        assert call_kwargs[1]["params"] == {"onDate": "2024-01-01"}

    def test_empty_reports_returns_empty_list(self):
        hook = _make_hook()
        response = _mock_response(200, {"result": {"reports": []}})

        with patch("requests.get", return_value=response):
            result = hook.get_builder_reports("2024-01-01")

        assert result == []

    def test_missing_result_key_returns_empty_list(self):
        hook = _make_hook()
        response = _mock_response(200, {})

        with patch("requests.get", return_value=response):
            result = hook.get_builder_reports("2024-01-01")

        assert result == []

    def test_retry_429_succeeds_on_second_attempt(self):
        hook = _make_hook()
        reports = [{"id": 1}]
        fail_response = _mock_response(429)
        ok_response = _mock_response(200, {"result": {"reports": reports}})

        with patch("requests.get", side_effect=[fail_response, ok_response]):
            with patch("time.sleep"):
                result = hook.get_builder_reports("2024-01-01")

        assert result == reports

    def test_three_retries_429_raises(self):
        hook = _make_hook()
        fail_response = _mock_response(429)

        with patch("requests.get", return_value=fail_response):
            with patch("time.sleep"):
                with pytest.raises(AirflowException, match="429"):
                    hook.get_builder_reports("2024-01-01")

    def test_5xx_triggers_retry(self):
        hook = _make_hook()
        fail_response = _mock_response(500)
        ok_response = _mock_response(200, {"result": {"reports": []}})

        with patch("requests.get", side_effect=[fail_response, ok_response]):
            with patch("time.sleep"):
                result = hook.get_builder_reports("2024-01-01")

        assert result == []

    def test_auth_header_sent(self):
        hook = _make_hook()
        response = _mock_response(200, {"result": {"reports": []}})

        with patch("requests.get", return_value=response) as mock_get:
            hook.get_builder_reports("2024-01-01")

        headers = mock_get.call_args[1]["headers"]
        assert headers["Authorization"] == "Bearer test-token"


class TestGetNewbuildingName:
    def test_success_returns_name(self):
        hook = _make_hook()
        response = _mock_response(
            200, {"result": {"newbuilding": {"displayName": "ЖК Тестовый"}}}
        )

        with patch("requests.get", return_value=response):
            name = hook.get_newbuilding_name(42)

        assert name == "ЖК Тестовый"

    def test_http_error_raises_airflow_exception(self):
        hook = _make_hook()
        fail_response = _mock_response(404)

        with patch("requests.get", return_value=fail_response):
            with patch("time.sleep"):
                with pytest.raises(AirflowException):
                    hook.get_newbuilding_name(42)

    def test_missing_key_raises_airflow_exception(self):
        hook = _make_hook()
        response = _mock_response(200, {"result": {}})

        with patch("requests.get", return_value=response):
            with pytest.raises(AirflowException, match="newbuilding"):
                hook.get_newbuilding_name(42)


class TestTestConnection:
    def test_success(self):
        hook = _make_hook()
        response = _mock_response(200, {"result": {"reports": []}})

        with patch("requests.get", return_value=response):
            ok, msg = hook.test_connection()

        assert ok is True
        assert msg == "Connection successful"

    def test_failure(self):
        hook = _make_hook()
        fail_response = _mock_response(401)

        with patch("requests.get", return_value=fail_response):
            with patch("time.sleep"):
                ok, msg = hook.test_connection()

        assert ok is False
        assert "401" in msg
