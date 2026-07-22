from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest
from airflow.exceptions import AirflowException, AirflowNotFoundException
from airflow.models import Connection

from airflow_provider_cian.accounts import (
    Account,
    _parse_accounts,
    list_accounts,
    resolve_cabinet_id,
    resolve_token,
)


def _make_conn(
    conn_id: str = "cian_test",
    host: str = "https://public-api.cian.ru",
    password: str | None = "test-token",
    login: str | None = None,
    accounts: list[dict] | None = None,
) -> Connection:
    extra = json.dumps({"accounts": accounts}) if accounts is not None else None
    return Connection(
        conn_id=conn_id,
        conn_type="http",
        host=host,
        password=password,
        login=login,
        extra=extra,
    )


class TestAccountDataclass:
    def test_sanitizes_dots_and_slashes(self):
        acc = Account(id="a.b/c")
        assert acc.id == "a_b_c"

    def test_valid_id_unchanged(self):
        acc = Account(id="abc-123_XYZ")
        assert acc.id == "abc-123_XYZ"

    def test_spaces_sanitized(self):
        acc = Account(id="my account")
        assert acc.id == "my_account"

    def test_special_chars_sanitized(self):
        acc = Account(id="a.b")
        assert acc.id == "a_b"


class TestParseAccounts:
    def test_returns_account_token_pairs(self):
        conn = _make_conn(accounts=[{"id": "abc", "token": "tok1"}, {"id": "xyz", "token": "tok2"}])
        result = _parse_accounts(conn)

        assert len(result) == 2
        assert result[0][0].id == "abc"
        assert result[0][1] == "tok1"
        assert result[1][0].id == "xyz"
        assert result[1][1] == "tok2"

    def test_entry_without_id_silently_skipped(self):
        conn = _make_conn(accounts=[{"token": "tok"}, {"id": "valid", "token": "t"}])
        result = _parse_accounts(conn)

        assert len(result) == 1
        assert result[0][0].id == "valid"

    def test_entry_with_empty_string_id_silently_skipped(self):
        conn = _make_conn(accounts=[{"id": "", "token": "tok"}, {"id": "valid", "token": "t"}])
        result = _parse_accounts(conn)

        assert len(result) == 1
        assert result[0][0].id == "valid"

    def test_entry_without_token_returns_none_token(self):
        conn = _make_conn(accounts=[{"id": "abc"}])
        result = _parse_accounts(conn)

        assert len(result) == 1
        assert result[0][1] is None

    def test_sanitizes_id(self):
        conn = _make_conn(accounts=[{"id": "a.b", "token": "tok"}])
        result = _parse_accounts(conn)

        assert result[0][0].id == "a_b"

    def test_no_accounts_key_returns_empty(self):
        conn = _make_conn(accounts=None)
        conn.extra = None
        result = _parse_accounts(conn)
        assert result == []

    def test_empty_accounts_list_returns_empty(self):
        conn = _make_conn(accounts=[])
        result = _parse_accounts(conn)
        assert result == []

    def test_no_dedup_returns_both_entries_for_duplicate_ids(self):
        """_parse_accounts returns all entries without deduplication."""
        conn = _make_conn(accounts=[{"id": "a.b", "token": "tok1"}, {"id": "a/b", "token": "tok2"}])
        result = _parse_accounts(conn)

        assert len(result) == 2
        assert result[0][0].id == "a_b"
        assert result[1][0].id == "a_b"


class TestListAccounts:
    def test_returns_accounts_with_sanitized_ids(self):
        conn = _make_conn(accounts=[{"id": "abc"}, {"id": "def"}])
        with patch("airflow.hooks.base.BaseHook.get_connection", return_value=conn):
            result = list_accounts("cian_test")

        assert len(result) == 2
        assert result[0].id == "abc"
        assert result[1].id == "def"
        assert all(isinstance(a, Account) for a in result)

    def test_ids_are_sanitized(self):
        conn = _make_conn(accounts=[{"id": "a.b/c"}, {"id": "x-y_z"}])
        with patch("airflow.hooks.base.BaseHook.get_connection", return_value=conn):
            result = list_accounts("cian_test")

        assert result[0].id == "a_b_c"
        assert result[1].id == "x-y_z"

    def test_returns_empty_list_when_no_accounts_key(self):
        conn = Connection(conn_id="cian_test", conn_type="http", host="https://example.com")
        with patch("airflow.hooks.base.BaseHook.get_connection", return_value=conn):
            result = list_accounts("cian_test")

        assert result == []

    def test_returns_empty_list_when_connection_not_found(self):
        with patch(
            "airflow.hooks.base.BaseHook.get_connection",
            side_effect=AirflowNotFoundException("not found"),
        ):
            with patch("airflow_provider_cian.accounts.log") as mock_log:
                result = list_accounts("nonexistent")

        assert result == []
        mock_log.warning.assert_called_once()

    def test_duplicate_sanitized_ids_keeps_first(self):
        # "a.b" and "a/b" both sanitize to "a_b"
        conn = _make_conn(accounts=[{"id": "a.b"}, {"id": "a/b"}])
        with patch("airflow.hooks.base.BaseHook.get_connection", return_value=conn):
            result = list_accounts("cian_test")

        assert len(result) == 1
        assert result[0].id == "a_b"

    def test_duplicate_sanitized_ids_logs_warning(self):
        conn = _make_conn(accounts=[{"id": "a.b"}, {"id": "a/b"}])
        with patch("airflow.hooks.base.BaseHook.get_connection", return_value=conn):
            with patch("airflow_provider_cian.accounts.log") as mock_log:
                list_accounts("cian_test")

        mock_log.warning.assert_called_once()
        warning_msg = mock_log.warning.call_args[0][0]
        assert "Duplicate" in warning_msg

    def test_entry_missing_id_is_skipped_and_logs_warning(self):
        conn = _make_conn(accounts=[{"token": "abc"}, {"id": "valid"}])
        with patch("airflow.hooks.base.BaseHook.get_connection", return_value=conn):
            with patch("airflow_provider_cian.accounts.log") as mock_log:
                result = list_accounts("cian_test")

        assert len(result) == 1
        assert result[0].id == "valid"
        mock_log.warning.assert_called_once()

    def test_entry_with_empty_string_id_is_skipped_and_logs_warning(self):
        conn = _make_conn(accounts=[{"id": "", "token": "abc"}, {"id": "valid"}])
        with patch("airflow.hooks.base.BaseHook.get_connection", return_value=conn):
            with patch("airflow_provider_cian.accounts.log") as mock_log:
                result = list_accounts("cian_test")

        assert len(result) == 1
        assert result[0].id == "valid"
        mock_log.warning.assert_called_once()

    def test_returns_empty_list_on_unexpected_error(self):
        with patch(
            "airflow.hooks.base.BaseHook.get_connection",
            side_effect=RuntimeError("unexpected"),
        ):
            with patch("airflow_provider_cian.accounts.log") as mock_log:
                result = list_accounts("cian_test")

        assert result == []
        mock_log.warning.assert_called_once()

    def test_does_not_expose_tokens(self):
        conn = _make_conn(accounts=[{"id": "abc", "token": "secret"}])
        with patch("airflow.hooks.base.BaseHook.get_connection", return_value=conn):
            result = list_accounts("cian_test")

        assert len(result) == 1
        # Account only has 'id' — no token attribute
        assert not hasattr(result[0], "token")


class TestResolveCabinetId:
    def test_multi_mode_returns_account_id_without_reading_connection(self):
        with patch("airflow.hooks.base.BaseHook.get_connection") as mock_get:
            result = resolve_cabinet_id("cian_test", "some_account")

        assert result == "some_account"
        mock_get.assert_not_called()

    def test_multi_mode_sanitizes_raw_account_id(self):
        with patch("airflow.hooks.base.BaseHook.get_connection") as mock_get:
            result = resolve_cabinet_id("cian_test", "a.b")

        assert result == "a_b"
        mock_get.assert_not_called()

    def test_multi_mode_sanitization_is_idempotent(self):
        with patch("airflow.hooks.base.BaseHook.get_connection") as mock_get:
            result = resolve_cabinet_id("cian_test", "a_b")

        assert result == "a_b"
        mock_get.assert_not_called()

    def test_multi_mode_empty_string_stays_empty(self):
        with patch("airflow.hooks.base.BaseHook.get_connection") as mock_get:
            result = resolve_cabinet_id("cian_test", "")

        assert result == ""
        mock_get.assert_not_called()

    def test_single_mode_with_login_returns_sanitized_id(self):
        conn = _make_conn(login="my.cabinet")
        with patch("airflow.hooks.base.BaseHook.get_connection", return_value=conn):
            result = resolve_cabinet_id("cian_test", None)

        assert result == "my_cabinet"

    def test_single_mode_without_login_returns_none(self):
        conn = _make_conn(login=None)
        with patch("airflow.hooks.base.BaseHook.get_connection", return_value=conn):
            result = resolve_cabinet_id("cian_test", None)

        assert result is None

    def test_single_mode_sanitizes_login(self):
        conn = _make_conn(login="a.b/c")
        with patch("airflow.hooks.base.BaseHook.get_connection", return_value=conn):
            result = resolve_cabinet_id("cian_test", None)

        assert result == "a_b_c"

    def test_single_mode_exception_propagates(self):
        """In single-mode, exceptions from get_connection propagate to the caller."""
        with patch(
            "airflow.hooks.base.BaseHook.get_connection",
            side_effect=AirflowNotFoundException("no conn"),
        ):
            with pytest.raises(AirflowNotFoundException):
                resolve_cabinet_id("nonexistent", None)


class TestResolveToken:
    def test_multi_mode_returns_matching_token(self):
        conn = _make_conn(accounts=[{"id": "abc", "token": "account-token"}, {"id": "xyz", "token": "other"}])
        result = resolve_token(conn, "abc")
        assert result == "account-token"

    def test_multi_mode_matches_sanitized_account_id(self):
        conn = _make_conn(accounts=[{"id": "a.b", "token": "dotted-token"}])
        # account_id is the sanitized form
        result = resolve_token(conn, "a_b")
        assert result == "dotted-token"

    def test_multi_mode_finds_token_by_raw_account_id(self):
        """A raw account_id ('a.b') matches an entry stored raw ('a.b'):
        both canonicalize to 'a_b'."""
        conn = _make_conn(accounts=[{"id": "a.b", "token": "dotted-token"}])
        result = resolve_token(conn, "a.b")
        assert result == "dotted-token"

    def test_multi_mode_raw_account_id_still_raises_for_absent_account(self):
        conn = _make_conn(conn_id="cian_test", accounts=[{"id": "a.b", "token": "tok"}])
        with pytest.raises(AirflowException, match="not found in connection"):
            resolve_token(conn, "x.y")

    def test_multi_mode_empty_account_id_raises_not_found(self):
        """account_id="" is multi-account mode (not None): it canonicalizes to ""
        and, absent an entry with an empty id, raises 'not found' rather than
        silently falling back to conn.password."""
        conn = _make_conn(conn_id="cian_test", accounts=[{"id": "abc", "token": "tok"}])
        with pytest.raises(AirflowException, match="not found in connection"):
            resolve_token(conn, "")

    def test_single_mode_returns_conn_password(self):
        conn = _make_conn(password="my-password")
        result = resolve_token(conn, None)
        assert result == "my-password"

    def test_multi_mode_account_not_found_raises(self):
        conn = _make_conn(accounts=[{"id": "abc", "token": "tok"}])
        with pytest.raises(AirflowException, match="not found in connection"):
            resolve_token(conn, "missing")

    def test_multi_mode_account_found_but_no_token_raises(self):
        conn = _make_conn(accounts=[{"id": "abc"}])
        with pytest.raises(AirflowException, match="missing required 'token' field"):
            resolve_token(conn, "abc")

    def test_single_mode_no_password_raises(self):
        conn = _make_conn(password=None)
        with pytest.raises(AirflowException, match="no password"):
            resolve_token(conn, None)

    def test_error_message_includes_account_id_and_conn_id(self):
        conn = _make_conn(conn_id="cian_test", accounts=[])
        with pytest.raises(AirflowException) as exc_info:
            resolve_token(conn, "nonexistent")

        msg = str(exc_info.value)
        assert "nonexistent" in msg
        assert "cian_test" in msg

    def test_ambiguous_collision_raises_without_warning(self):
        """Two entries collapsing to the same sanitized id ('a.b' and 'a/b' both
        -> 'a_b') are ambiguous at runtime: resolve_token raises AirflowException
        instead of silently picking the first, and still logs no WARNING
        (execution-time policy — it fails via exception, not a log line)."""
        conn = _make_conn(
            conn_id="cian_test",
            accounts=[{"id": "a.b", "token": "tok1"}, {"id": "a/b", "token": "tok2"}],
        )
        with patch("airflow_provider_cian.accounts.log") as mock_log:
            with pytest.raises(AirflowException) as exc_info:
                resolve_token(conn, "a.b")

        msg = str(exc_info.value)
        # diagnostic completeness: original account_id, conn_id, canonical id, multiplicity
        assert "a.b" in msg  # original account_id
        assert "cian_test" in msg  # conn_id
        assert "a_b" in msg  # canonical (sanitized) id
        assert "2" in msg and "collapse" in msg  # match count / ambiguity signal
        mock_log.warning.assert_not_called()

    def test_no_warnings_logged_when_entry_missing_id(self):
        """resolve_token must not log warnings for entries without id."""
        conn = _make_conn(accounts=[{"token": "tok"}, {"id": "valid", "token": "valid-tok"}])
        with patch("airflow_provider_cian.accounts.log") as mock_log:
            result = resolve_token(conn, "valid")

        assert result == "valid-tok"
        mock_log.warning.assert_not_called()

    def test_exact_error_message_account_not_found(self):
        conn = _make_conn(conn_id="cian_test", accounts=[{"id": "abc", "token": "tok"}])
        with pytest.raises(AirflowException) as exc_info:
            resolve_token(conn, "missing")

        assert str(exc_info.value) == (
            "Account id='missing' not found in connection 'cian_test' extra.accounts"
        )

    def test_exact_error_message_token_missing(self):
        conn = _make_conn(conn_id="cian_test", accounts=[{"id": "abc"}])
        with pytest.raises(AirflowException) as exc_info:
            resolve_token(conn, "abc")

        assert str(exc_info.value) == (
            "Account id='abc' found in connection 'cian_test' but is missing required 'token' field"
        )

    def test_exact_error_message_no_password(self):
        conn = _make_conn(conn_id="cian_test", password=None)
        with pytest.raises(AirflowException) as exc_info:
            resolve_token(conn, None)

        assert str(exc_info.value) == (
            "Connection 'cian_test' has no password (token) set and no account_id was provided."
        )
