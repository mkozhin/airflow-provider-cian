from __future__ import annotations

import logging
import re
from dataclasses import dataclass

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.models import Connection

log = logging.getLogger(__name__)


@dataclass
class Account:
    """Represents a Cian account (cabinet). The `id` is sanitized on creation."""

    id: str

    def __post_init__(self) -> None:
        self.id = re.sub(r"[^\w-]", "_", self.id)


def _parse_accounts(conn: Connection) -> list[tuple[Account, str | None]]:
    """Parse accounts from connection extra field.

    Returns (Account, token_or_None) pairs. No logging, no dedup.
    Silently skips entries without 'id'. Callers apply policy (logging, dedup).
    Used by resolve_token for execution-time resolution.
    """
    raw_accounts = conn.extra_dejson.get("accounts", [])
    result: list[tuple[Account, str | None]] = []
    for entry in raw_accounts:
        if not entry.get("id"):
            continue
        acc = Account(id=entry["id"])
        token: str | None = entry.get("token") or None
        result.append((acc, token))
    return result


def list_accounts(conn_id: str) -> list[Account]:
    """Read accounts from the Airflow connection extra field.

    Returns a list of Account objects (sanitized ids). Returns [] on any error
    (missing connection, missing key, etc.) — this function does not raise.

    Duplicate sanitized ids are deduplicated: only the first account is kept
    and a WARNING is logged.
    """
    try:
        conn = BaseHook.get_connection(conn_id)
        raw_accounts = conn.extra_dejson.get("accounts", [])
        for entry in raw_accounts:
            if not entry.get("id"):
                log.warning("Skipping account entry missing required 'id' key; entry has keys: %r", list(entry.keys()))
        accounts: list[Account] = []
        seen: set[str] = set()
        for acc, _ in _parse_accounts(conn):
            if acc.id in seen:
                log.warning(
                    "Duplicate account id after sanitization: %r. "
                    "Keeping the first, skipping the second.",
                    acc.id,
                )
            else:
                seen.add(acc.id)
                accounts.append(acc)
        return accounts
    except Exception:
        log.warning(
            "Could not load accounts from connection %r. Returning empty list.",
            conn_id,
            exc_info=True,
        )
        return []


def resolve_cabinet_id(conn_id: str, account_id: str | None) -> str | None:
    """Resolve the cabinet id for an operation.

    In multi-account mode (account_id is not None): returns account_id directly,
    without reading the connection.
    In single-account mode (account_id is None): reads the connection lazily and
    returns Account(id=conn.login).id if conn.login is set, otherwise None.
    """
    if account_id is not None:
        return account_id
    conn = BaseHook.get_connection(conn_id)
    if conn.login:
        return Account(id=conn.login).id
    return None


def resolve_token(conn: Connection, account_id: str | None) -> str:
    """Resolve the authentication token for a request.

    In multi-account mode (account_id is not None): finds the first account
    matching account_id in conn.extra.accounts and returns its token.
    In single-account mode (account_id is None): returns conn.password.

    Raises AirflowException with exact error messages matching current _make_request
    behavior. No warnings are logged (execution-time policy).
    """
    if account_id is not None:
        for acc, token in _parse_accounts(conn):
            if acc.id == account_id:
                if not token:
                    raise AirflowException(
                        f"Account id={account_id!r} found in connection "
                        f"{conn.conn_id!r} but is missing required 'token' field"
                    )
                return token
        raise AirflowException(
            f"Account id={account_id!r} not found in connection {conn.conn_id!r} extra.accounts"
        )
    token = conn.password
    if not token:
        raise AirflowException(
            f"Connection {conn.conn_id!r} has no password (token) set "
            "and no account_id was provided."
        )
    return token
