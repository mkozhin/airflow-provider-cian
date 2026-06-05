from __future__ import annotations

import time
from datetime import date

import requests
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook


class CianNotFoundError(AirflowException):
    """Raised when the Cian API responds with a 'not found' status code."""


class CianHook(BaseHook):
    conn_name_attr = "cian_conn_id"
    default_conn_name = "cian_default"
    conn_type = "http"
    hook_name = "Cian Builder API"

    def __init__(self, cian_conn_id: str = default_conn_name) -> None:
        super().__init__()
        self.cian_conn_id = cian_conn_id

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
            self.log.warning("Newbuilding id=%s not found (400), returning 'Неизвестно'", newbuilding_id)
            return "Неизвестно"
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

            if not_found_codes and resp.status_code in not_found_codes:
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
