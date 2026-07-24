from __future__ import annotations

import csv
import json
import os
import re
from datetime import datetime as _datetime, timedelta, timezone

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator

from airflow_provider_cian.accounts import resolve_cabinet_id, sanitize_id
from airflow_provider_cian.hooks.cian import CianHook

_MSK = timezone(timedelta(hours=3))
_OUTPUT_FORMATS = ("json", "csv")

# Cian sends sub-second digits at an unstable precision, which fromisoformat()
# rejects before Python 3.11 — that killed a prod task, and nothing downstream reads
# the fraction, so it is dropped. Bounded on both sides (leading date + HH:MM:SS
# before it, an offset sign or end of string after it) so nothing else is rewritten
# and a malformed value is never patched into a valid one; [.,] are both ISO-8601
# fraction separators, [T ] both date/time separators; [0-9], not Unicode-aware \d.
# See ADR-0005.
_FRACTIONAL_SECONDS_RE = re.compile(
    r"(?<=^[0-9]{4}-[0-9]{2}-[0-9]{2}[T ][0-9]{2}:[0-9]{2}:[0-9]{2})[.,][0-9]+(?=[+-]|$)"
)

_CSV_FIELDS = [
    "id",
    "account_id",
    "newbuilding_id",
    "newbuilding_name",
    "date",
    "datetime",
    "action_type",
    "searcher_phone",
    "searcher_ct_phone",
    "builder_user_ct_phone",
    "builder_user_phone",
    "builder_sip_uri",
    "call_duration",
    "tariff_price",
    "auction_bet",
    "cashback_spent",
    "billing_price",
    "has_claim",
    "is_targeted",
]
# _CSV_FIELDS is the canonical base set for Builder Report (CSV uses exactly these 19 fields).
# _SNAPSHOT_FIELD is the only optional extension — JSON-only output (see ADR-0001).
_SNAPSHOT_FIELD = "snapshot_ts"


def _parse_event_datetime(raw_dt, record_id) -> str:
    """Turn a raw Cian ``date`` value into the documented MSK second-precision string.

    Every failure mode of the field (missing, non-string, unparsable) raises
    ``AirflowException`` naming the record ``id``; see ADR-0005.
    """
    if raw_dt is None:
        raise AirflowException(
            f"Record id={record_id} is missing required field 'date'"
        )
    if not isinstance(raw_dt, str):
        raise AirflowException(
            f"Record id={record_id} has a non-string 'date': {raw_dt!r}"
        )

    normalized_dt = _FRACTIONAL_SECONDS_RE.sub("", raw_dt.replace("Z", "+00:00"))
    try:
        dt = _datetime.fromisoformat(normalized_dt)
    except ValueError as e:
        # Quote raw_dt: the string reaching the parser was already rewritten
        # ("Z" -> "+00:00", fraction dropped), and it was the verbatim value that
        # let the prod incident be root-caused from the log alone.
        raise AirflowException(
            f"Record id={record_id} has an unparsable 'date': {raw_dt!r}"
        ) from e

    dt = dt.replace(tzinfo=_MSK) if dt.tzinfo is None else dt.astimezone(_MSK)
    # microsecond=0 makes the documented output structural: the regex covers only
    # the extended format, while 3.11+ parses shapes that would leak a fraction.
    return dt.replace(microsecond=0).isoformat()


class CianBuilderReportsOperator(BaseOperator):
    template_fields = ("date", "cian_conn_id")
    ui_color = "#e8f5e9"

    def __init__(
        self,
        *,
        cian_conn_id: str = "cian_default",
        date: str,
        base_dir: str = "/tmp/cian",
        output_format: str = "json",
        account_id: str | None = None,
        add_snapshot_ts: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        if output_format not in _OUTPUT_FORMATS:
            raise ValueError(f"output_format must be one of {_OUTPUT_FORMATS}, got {output_format!r}")
        self.cian_conn_id = cian_conn_id
        self.date = date
        self.base_dir = base_dir
        self.output_format = output_format
        self.account_id = account_id
        self.add_snapshot_ts = add_snapshot_ts

    def execute(self, context) -> dict[str, str] | None:
        """Fetch reports for ``self.date`` and materialise them on disk.

        Returns a self-describing ``{"date": ..., "path": ...}`` dict for a day
        that has data, or ``None`` for an empty day. On an empty day no file and
        no run directory are created, and because Airflow does not write an XCom
        for ``None``, the day drops out of downstream mapped-task expansion
        entirely (nothing is uploaded).
        """
        # Validate `date` FIRST — it is a template field that lands verbatim in the
        # output filename (`_build_path`), BQ partition names ($YYYYMMDD) and S3
        # _year=/_month=/_day= paths, so it is contractually required to be a
        # zero-padded ISO date. This is also the only unsanitized path component
        # (run_id/cabinet_id are sanitized), so the shape check closes path
        # traversal via `date` too. ASCII [0-9] (not \d, which is Unicode-aware
        # and would accept full-width digits); isinstance guards non-str/None;
        # strptime rejects calendar-impossible dates (2026-13-45, 2024-02-30).
        if not (isinstance(self.date, str)
                and re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", self.date)):
            raise AirflowException(
                f"date must be an ISO date string (YYYY-MM-DD), got {self.date!r}"
            )
        try:
            _datetime.strptime(self.date, "%Y-%m-%d")
        except ValueError as e:
            raise AirflowException(
                f"date must be a valid calendar date (YYYY-MM-DD), got {self.date!r}"
            ) from e

        snapshot_ts = (
            context["dag_run"].start_date.strftime("%Y-%m-%dT%H:%M:%S")
            if self.add_snapshot_ts and self.output_format == "json" else None
        )

        # cabinet_id is the canonical (sanitized) Account ID — same value the public
        # account_id field carries; resolve_cabinet_id() sanitizes it internally.
        cabinet_id = resolve_cabinet_id(self.cian_conn_id, self.account_id)
        # Fail fast BEFORE any hook/API side effects. `not cabinet_id` rejects both
        # None (single-account without login) and "" (empty account_id, which
        # os.path.join would silently swallow, dropping the cabinet path segment).
        if not cabinet_id:
            raise AirflowException(
                f"Account ID is required: set 'login' in connection {self.cian_conn_id!r} "
                "or pass a non-empty account_id"
            )
        hook = CianHook(cian_conn_id=self.cian_conn_id, account_id=self.account_id)

        output_path = self._build_path(context["run_id"], cabinet_id)

        # Remove a stale file from a previous attempt BEFORE deciding on emptiness:
        # on a retry where the data has since disappeared, an outdated file would
        # otherwise be left behind up top.
        if os.path.exists(output_path):
            os.remove(output_path)

        records = hook.get_builder_reports(self.date)
        if not records:
            self.log.info("Cian returned no reports for %s — nothing to write", self.date)
            return None

        # makedirs happens AFTER the emptiness check so that an empty day leaves
        # neither a file nor a fresh run directory behind (a run directory may
        # already exist because of neighbouring dates — that is fine).
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        enriched = self._enrich(records, hook, cabinet_id, snapshot_ts)
        self._write(enriched, output_path)

        return {"date": self.date, "path": output_path}

    def _build_path(self, run_id: str, cabinet_id: str) -> str:
        safe_run_id = sanitize_id(run_id)
        ext = "json" if self.output_format == "json" else "csv"
        return os.path.join(self.base_dir, cabinet_id, safe_run_id, f"{self.date}.{ext}")

    def _enrich(
        self, records: list[dict], hook: CianHook, account_id: str, snapshot_ts: str | None = None
    ) -> list[dict]:
        unique_ids = {r["newbuildingId"] for r in records if "newbuildingId" in r}
        name_cache: dict[int, str] = {nid: hook.get_newbuilding_name(nid) for nid in unique_ids}

        result = []
        for record in records:
            record_id = record.get("id")
            nid = record.get("newbuildingId")
            billing_price = record.get("billingPrice", 0) or 0
            dt_str = _parse_event_datetime(record.get("date"), record_id)

            row = {
                "id": record_id,
                "account_id": account_id,
                "newbuilding_id": nid,
                "newbuilding_name": name_cache.get(nid, ""),
                "date": self.date,
                "datetime": dt_str,
                "action_type": record.get("actionType"),
                "searcher_phone": record.get("searcherPhone"),
                "searcher_ct_phone": record.get("searcherCtPhone"),
                "builder_user_ct_phone": record.get("builderUserCtPhone"),
                "builder_user_phone": record.get("builderUserPhone"),
                "builder_sip_uri": record.get("builderSipUri"),
                "call_duration": record.get("callDuration"),
                "tariff_price": record.get("tariffPrice"),
                "auction_bet": record.get("auctionBet"),
                "cashback_spent": record.get("cashbackSpent"),
                "billing_price": billing_price,
                "has_claim": record.get("hasClaim"),
                "is_targeted": billing_price > 0,
            }
            if snapshot_ts:
                row[_SNAPSHOT_FIELD] = snapshot_ts
            result.append(row)
        return result

    def _write(self, records: list[dict], path: str) -> None:
        if self.output_format == "json":
            with open(path, "w", encoding="utf-8") as f:
                for row in records:
                    f.write(json.dumps(row, ensure_ascii=False) + "\n")
        else:
            with open(path, "w", encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=_CSV_FIELDS, quoting=csv.QUOTE_ALL)
                writer.writeheader()
                writer.writerows(records)
