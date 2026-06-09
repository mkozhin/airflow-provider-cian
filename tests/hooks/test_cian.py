from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from airflow.exceptions import AirflowException
from airflow.models import Connection

from airflow_provider_cian.hooks.cian import Account, CianHook, CianNotFoundError, get_accounts


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


def _make_hook_with_accounts(accounts: list[dict], password: str = "default-token") -> CianHook:
    """Create a CianHook whose connection has the given accounts list in extra."""
    import json

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


class TestAccountDataclass:
    def test_sanitizes_id_with_dots_and_slashes(self):
        acc = Account(id="a.b/c")
        assert acc.id == "a_b_c"

    def test_valid_id_unchanged(self):
        acc = Account(id="abc-123_XYZ")
        assert acc.id == "abc-123_XYZ"

    def test_spaces_sanitized(self):
        acc = Account(id="my account")
        assert acc.id == "my_account"


class TestCianHookWithAccountId:
    def test_uses_account_token_when_account_id_matches(self):
        accounts = [{"id": "abc", "token": "account-token"}, {"id": "xyz", "token": "other-token"}]
        hook = _make_hook_with_accounts(accounts, password="default-token")
        hook.account_id = "abc"

        response = _mock_response(200, {"result": {"reports": []}})
        with patch("requests.get", return_value=response) as mock_get:
            with patch("time.sleep"):
                hook.get_builder_reports("2024-01-01")

        headers = mock_get.call_args[1]["headers"]
        assert headers["Authorization"] == "Bearer account-token"

    def test_raises_airflow_exception_when_account_id_missing(self):
        accounts = [{"id": "abc", "token": "account-token"}]
        hook = _make_hook_with_accounts(accounts, password="default-token")
        hook.account_id = "missing"

        with pytest.raises(AirflowException, match="missing"):
            hook.get_builder_reports("2024-01-01")

    def test_without_account_id_uses_conn_password(self):
        accounts = [{"id": "abc", "token": "account-token"}]
        hook = _make_hook_with_accounts(accounts, password="default-token")
        # no account_id set (default None)

        response = _mock_response(200, {"result": {"reports": []}})
        with patch("requests.get", return_value=response) as mock_get:
            with patch("time.sleep"):
                hook.get_builder_reports("2024-01-01")

        headers = mock_get.call_args[1]["headers"]
        assert headers["Authorization"] == "Bearer default-token"

    def test_account_id_matched_by_sanitized_id(self):
        """Account with id 'a.b' in extra should be found when account_id='a_b'."""
        import json

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

    def test_error_message_includes_account_id_and_conn_id(self):
        hook = _make_hook_with_accounts([], password="default-token")
        hook.account_id = "nonexistent"

        with pytest.raises(AirflowException) as exc_info:
            hook.get_builder_reports("2024-01-01")

        msg = str(exc_info.value)
        assert "nonexistent" in msg
        assert "cian_test" in msg


class TestGetAccounts:
    def _make_conn_with_accounts(self, accounts: list[dict]) -> Connection:
        import json

        return Connection(
            conn_id="cian_test",
            conn_type="http",
            host="https://public-api.cian.ru",
            password="test-token",
            extra=json.dumps({"accounts": accounts}),
        )

    def test_returns_list_of_accounts_with_sanitized_ids(self):
        conn = self._make_conn_with_accounts([{"id": "abc"}, {"id": "def"}])
        with patch("airflow.hooks.base.BaseHook.get_connection", return_value=conn):
            result = get_accounts("cian_test")

        assert len(result) == 2
        assert result[0].id == "abc"
        assert result[1].id == "def"
        assert all(isinstance(a, Account) for a in result)

    def test_ids_are_sanitized(self):
        conn = self._make_conn_with_accounts([{"id": "a.b/c"}, {"id": "x-y_z"}])
        with patch("airflow.hooks.base.BaseHook.get_connection", return_value=conn):
            result = get_accounts("cian_test")

        assert result[0].id == "a_b_c"
        assert result[1].id == "x-y_z"

    def test_returns_empty_list_when_no_accounts_key(self):
        conn = Connection(
            conn_id="cian_test",
            conn_type="http",
            host="https://public-api.cian.ru",
        )
        with patch("airflow.hooks.base.BaseHook.get_connection", return_value=conn):
            result = get_accounts("cian_test")

        assert result == []

    def test_returns_empty_list_when_connection_not_found(self):
        from airflow.exceptions import AirflowNotFoundException

        with patch(
            "airflow.hooks.base.BaseHook.get_connection",
            side_effect=AirflowNotFoundException("not found"),
        ):
            result = get_accounts("nonexistent")

        assert result == []

    def test_duplicate_sanitized_ids_keeps_first(self):
        # "a.b" and "a/b" both sanitize to "a_b"
        conn = self._make_conn_with_accounts([{"id": "a.b"}, {"id": "a/b"}])
        with patch("airflow.hooks.base.BaseHook.get_connection", return_value=conn):
            result = get_accounts("cian_test")

        assert len(result) == 1
        assert result[0].id == "a_b"

    def test_duplicate_sanitized_ids_logs_warning(self):
        conn = self._make_conn_with_accounts([{"id": "a.b"}, {"id": "a/b"}])
        with patch("airflow.hooks.base.BaseHook.get_connection", return_value=conn):
            with patch("airflow_provider_cian.hooks.cian.log") as mock_log:
                get_accounts("cian_test")

        mock_log.warning.assert_called_once()
        warning_msg = mock_log.warning.call_args[0][0]
        assert "Duplicate" in warning_msg
