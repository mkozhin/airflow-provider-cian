from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest
from airflow.exceptions import AirflowException
from airflow.models import Connection

from airflow_provider_cian.accounts import Account
from airflow_provider_cian.hooks.cian import CianHook, CianNotFoundError


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


_UNSET = object()


def _mock_response(status_code: int, json_data: object = _UNSET) -> MagicMock:
    # Use a sentinel default so callers can explicitly pass [] or None as the
    # body; `json_data or {}` would collapse both of those into {}.
    body = {} if json_data is _UNSET else json_data
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = body
    resp.text = str(body)
    return resp


def _make_hook_with_accounts(accounts: list[dict], password: str = "default-token") -> CianHook:
    """Create a CianHook whose connection has the given accounts list in extra."""
    hook = CianHook(cian_conn_id="cian_test")
    conn = Connection(
        conn_id="cian_test",
        conn_type="http",
        host="https://public-api.cian.ru",
        password=password,
        extra=json.dumps({"accounts": accounts}),
    )
    hook.get_connection = MagicMock(return_value=conn)
    return hook


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

    def test_missing_result_key_raises(self):
        hook = _make_hook()
        response = _mock_response(200, {})

        with patch("requests.get", return_value=response):
            with pytest.raises(AirflowException, match="onDate=2024-01-01"):
                hook.get_builder_reports("2024-01-01")

    def test_reports_not_a_list_raises(self):
        hook = _make_hook()
        response = _mock_response(200, {"result": {"reports": {}}})

        with patch("requests.get", return_value=response):
            with pytest.raises(AirflowException, match="result.reports"):
                hook.get_builder_reports("2024-01-01")

    def test_errors_body_with_200_raises(self):
        hook = _make_hook()
        response = _mock_response(200, {"errors": [{"code": "boom"}]})

        with patch("requests.get", return_value=response):
            with pytest.raises(AirflowException, match="Unexpected Cian response"):
                hook.get_builder_reports("2024-01-01")

    def test_top_level_list_body_raises(self):
        hook = _make_hook()
        response = _mock_response(200, [])

        with patch("requests.get", return_value=response):
            with pytest.raises(AirflowException, match="Unexpected Cian response"):
                hook.get_builder_reports("2024-01-01")

    def test_top_level_null_body_raises(self):
        hook = _make_hook()
        response = _mock_response(200, None)

        with patch("requests.get", return_value=response):
            with pytest.raises(AirflowException, match="Unexpected Cian response"):
                hook.get_builder_reports("2024-01-01")

    def test_broken_response_makes_test_connection_return_false(self):
        hook = _make_hook()
        response = _mock_response(200, {"errors": [{"code": "boom"}]})

        with patch("requests.get", return_value=response):
            ok, msg = hook.test_connection()

        assert ok is False
        assert "Unexpected Cian response" in msg

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
            200, {"result": {"newbuilding": {"name": "ЖК Тестовый"}}}
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

    def test_not_found_400_returns_неизвестно(self):
        hook = _make_hook()
        fail_response = _mock_response(400)

        with patch("requests.get", return_value=fail_response):
            with patch("time.sleep"):
                with patch("logging.Logger.warning") as mock_warning:
                    name = hook.get_newbuilding_name(99)

        assert name == "Неизвестно"
        mock_warning.assert_called_once()

    def test_5xx_still_raises_after_not_found_fix(self):
        hook = _make_hook()
        fail_response = _mock_response(500)

        with patch("requests.get", return_value=fail_response):
            with patch("time.sleep"):
                with pytest.raises(AirflowException):
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


class TestCianNotFoundError:
    def test_is_subclass_of_airflow_exception(self):
        assert issubclass(CianNotFoundError, AirflowException)


class TestCianHookWithAccountId:
    def test_uses_account_token_when_account_id_matches(self):
        accounts = [{"id": "abc", "token": "account-token"}, {"id": "xyz", "token": "other-token"}]
        hook = CianHook(cian_conn_id="cian_test", account_id="abc")
        conn = Connection(
            conn_id="cian_test",
            conn_type="http",
            host="https://public-api.cian.ru",
            password="default-token",
            extra=json.dumps({"accounts": accounts}),
        )
        hook.get_connection = MagicMock(return_value=conn)

        response = _mock_response(200, {"result": {"reports": []}})
        with patch("requests.get", return_value=response) as mock_get:
            with patch("time.sleep"):
                hook.get_builder_reports("2024-01-01")

        headers = mock_get.call_args[1]["headers"]
        assert headers["Authorization"] == "Bearer account-token"

    def test_raises_airflow_exception_when_account_id_missing(self):
        accounts = [{"id": "abc", "token": "account-token"}]
        hook = CianHook(cian_conn_id="cian_test", account_id="missing")
        conn = Connection(
            conn_id="cian_test",
            conn_type="http",
            host="https://public-api.cian.ru",
            password="default-token",
            extra=json.dumps({"accounts": accounts}),
        )
        hook.get_connection = MagicMock(return_value=conn)

        with pytest.raises(AirflowException, match="missing"):
            hook.get_builder_reports("2024-01-01")

    def test_without_account_id_uses_conn_password(self):
        accounts = [{"id": "abc", "token": "account-token"}]
        hook = _make_hook_with_accounts(accounts, password="default-token")
        # account_id defaults to None in constructor

        response = _mock_response(200, {"result": {"reports": []}})
        with patch("requests.get", return_value=response) as mock_get:
            with patch("time.sleep"):
                hook.get_builder_reports("2024-01-01")

        headers = mock_get.call_args[1]["headers"]
        assert headers["Authorization"] == "Bearer default-token"

    def test_raises_when_no_account_id_and_conn_password_is_none(self):
        """Single-account mode with no password set must raise AirflowException, not Bearer None."""
        hook = CianHook(cian_conn_id="cian_test")
        conn = Connection(
            conn_id="cian_test",
            conn_type="http",
            host="https://public-api.cian.ru",
            password=None,
        )
        hook.get_connection = MagicMock(return_value=conn)

        with pytest.raises(AirflowException, match="no password"):
            hook.get_builder_reports("2024-01-01")

    def test_account_id_matched_by_sanitized_id(self):
        """Account with id 'a.b' in extra should be found when account_id='a_b'."""
        hook = CianHook(cian_conn_id="cian_test", account_id="a_b")
        conn = Connection(
            conn_id="cian_test",
            conn_type="http",
            host="https://public-api.cian.ru",
            password="default-token",
            extra=json.dumps({"accounts": [{"id": "a.b", "token": "dotted-token"}]}),
        )
        hook.get_connection = MagicMock(return_value=conn)

        response = _mock_response(200, {"result": {"reports": []}})
        with patch("requests.get", return_value=response) as mock_get:
            with patch("time.sleep"):
                hook.get_builder_reports("2024-01-01")

        headers = mock_get.call_args[1]["headers"]
        assert headers["Authorization"] == "Bearer dotted-token"

    def test_raises_when_account_found_but_token_missing(self):
        """Account entry found by id but lacking 'token' key must raise AirflowException."""
        hook = CianHook(cian_conn_id="cian_test", account_id="abc")
        conn = Connection(
            conn_id="cian_test",
            conn_type="http",
            host="https://public-api.cian.ru",
            password="default-token",
            extra=json.dumps({"accounts": [{"id": "abc"}]}),
        )
        hook.get_connection = MagicMock(return_value=conn)

        with pytest.raises(AirflowException, match="missing required 'token' field"):
            hook.get_builder_reports("2024-01-01")

    def test_error_message_includes_account_id_and_conn_id(self):
        hook = CianHook(cian_conn_id="cian_test", account_id="nonexistent")
        conn = Connection(
            conn_id="cian_test",
            conn_type="http",
            host="https://public-api.cian.ru",
            password="default-token",
            extra=json.dumps({"accounts": []}),
        )
        hook.get_connection = MagicMock(return_value=conn)

        with pytest.raises(AirflowException) as exc_info:
            hook.get_builder_reports("2024-01-01")

        msg = str(exc_info.value)
        assert "nonexistent" in msg
        assert "cian_test" in msg


