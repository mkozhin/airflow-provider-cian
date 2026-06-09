from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass
from datetime import date

import requests
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook

log = logging.getLogger(__name__)


class CianNotFoundError(AirflowException):
    """Raised when the Cian API responds with a 'not found' status code."""


@dataclass
class Account:
    """Represents a Cian account (cabinet). The `id` is sanitized on creation."""

    id: str

    def __post_init__(self) -> None:
        self.id = re.sub(r"[^\w-]", "_", self.id)


def get_accounts(conn_id: str) -> list[Account]:
    """Read accounts from the Airflow connection extra field.

    Returns a list of Account objects (sanitized ids). Returns [] on any error
    (missing connection, missing key, etc.) — callers must not raise.

    Duplicate sanitized ids are deduplicated: only the first account is kept
    and a WARNING is logged.
    """
    try:
        conn = BaseHook.get_connection(conn_id)
        raw_accounts = conn.extra_dejson.get("accounts", [])
        accounts: list[Account] = []
        seen: dict[str, str] = {}  # sanitized_id -> original_id
        for entry in raw_accounts:
            original_id = entry["id"]
            acc = Account(id=original_id)
            if acc.id in seen:
                log.warning(
                    "Duplicate account id after sanitization: %r and %r both become %r. "
                    "Keeping the first, skipping the second.",
                    seen[acc.id],
                    original_id,
                    acc.id,
                )
            else:
                seen[acc.id] = original_id
                accounts.append(acc)
        return accounts
    except Exception:
        log.warning(
            "Could not load accounts from connection %r. Returning empty list.",
            conn_id,
            exc_info=True,
        )
        return []


class CianHook(BaseHook):
    conn_name_attr = "cian_conn_id"
    default_conn_name = "cian_default"
    conn_type = "http"
    hook_name = "Cian Builder API"

    def __init__(self, cian_conn_id: str = default_conn_name, account_id: str | None = None) -> None:
        super().__init__()
        self.cian_conn_id = cian_conn_id
        self.account_id = account_id

    def get_builder_reports(self, on_date: str) -> list[dict]:
        data = self._make_request("/v1/get-builder-reports/", {"onDate": on_date})
        return data.get("result", {}).get("reports", [])

    def get_newbuilding_name(self, newbuilding_id: int) -> str:
        try:
            data = self._make_request(
                "/v1/get-newbuilding/",
                {"newbuildingId": newbuilding_id},
                not_found_codes=(400,),
            )
            return data["result"]["newbuilding"]["name"]
        except CianNotFoundError:
            self.log.warning("Newbuilding id=%s not found (400), using fallback name", newbuilding_id)
            return "Неизвестно"
        except AirflowException:
            raise
        except Exception as e:
            raise AirflowException(f"Failed to get newbuilding name for id={newbuilding_id}: {e}") from e

    def test_connection(self) -> tuple[bool, str]:
        try:
            self.get_builder_reports(on_date=date.today().isoformat())
            return True, "Connection successful"
        except Exception as e:
            return False, str(e)

    def _make_request(self, path: str, params: dict, not_found_codes: tuple[int, ...] = ()) -> dict:
        conn = self.get_connection(self.cian_conn_id)
        base_url = conn.host.rstrip("/")

        if self.account_id is not None:
            raw_accounts = conn.extra_dejson.get("accounts", [])
            matched_token: str | None = None
            for entry in raw_accounts:
                sanitized = re.sub(r"[^\w-]", "_", entry["id"])
                if sanitized == self.account_id:
                    matched_token = entry["token"]
                    break
            if matched_token is None:
                raise AirflowException(
                    f"Account id={self.account_id!r} not found in connection {self.cian_conn_id!r} extra.accounts"
                )
            token = matched_token
        else:
            token = conn.password

        headers = {"Authorization": f"Bearer {token}"}
        url = f"{base_url}{path}"

        backoff_delays = [1, 2, 4]
        last_exc: Exception | None = None

        for attempt in range(len(backoff_delays) + 1):
            time.sleep(0.1)
            try:
                resp = requests.get(url, params=params, headers=headers, timeout=30)
            except requests.RequestException as e:
                raise AirflowException(f"Request to {url} failed: {e}") from e

            if resp.status_code == 200:
                return resp.json()

            # not_found_codes bypass retry — these are non-transient errors
            if resp.status_code in not_found_codes:
                raise CianNotFoundError(
                    f"Cian API returned {resp.status_code} (not found) for {url}"
                )

            if resp.status_code in (429, 500, 502, 503, 504):
                last_exc = AirflowException(
                    f"Cian API returned {resp.status_code} for {url} (attempt {attempt + 1})"
                )
                if attempt < len(backoff_delays):
                    time.sleep(backoff_delays[attempt])
                    continue
                raise last_exc

            raise AirflowException(
                f"Cian API error {resp.status_code} for {url}: {resp.text[:200]}"
            )

        raise AirflowException(f"All retry attempts exhausted for {url}")
