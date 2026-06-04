from __future__ import annotations

import csv
import json
import os
import re
from datetime import datetime, timedelta, timezone

from airflow.models import BaseOperator

from airflow_provider_cian.hooks.cian import CianHook

_MSK = timezone(timedelta(hours=3))
_OUTPUT_FORMATS = ("json", "csv")

_CSV_FIELDS = [
    "id",
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
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        if output_format not in _OUTPUT_FORMATS:
            raise ValueError(f"output_format must be one of {_OUTPUT_FORMATS}, got {output_format!r}")
        self.cian_conn_id = cian_conn_id
        self.date = date
        self.base_dir = base_dir
        self.output_format = output_format

    def execute(self, context) -> str:
        output_path = self._build_path(context["run_id"])

        if os.path.exists(output_path):
            os.remove(output_path)

        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        hook = CianHook(cian_conn_id=self.cian_conn_id)
        records = hook.get_builder_reports(self.date)
        enriched = self._enrich(records, hook)
        self._write(enriched, output_path)

        return output_path

    def _build_path(self, run_id: str) -> str:
        safe_run_id = re.sub(r"[^\w-]", "_", run_id)
        ext = "json" if self.output_format == "json" else "csv"
        return os.path.join(self.base_dir, safe_run_id, f"{self.date}.{ext}")

    def _enrich(self, records: list[dict], hook: CianHook) -> list[dict]:
        unique_ids = {r["newbuildingId"] for r in records if "newbuildingId" in r}
        name_cache: dict[int, str] = {nid: hook.get_newbuilding_name(nid) for nid in unique_ids}

        result = []
        for record in records:
            nid = record.get("newbuildingId")
            billing_price = record.get("billingPrice", 0) or 0

            raw_dt = record.get("date")
            if raw_dt:
                dt = datetime.fromisoformat(raw_dt)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=_MSK)
                dt_str = dt.isoformat()
                date_str = dt.date().isoformat()
            else:
                dt_str = None
                date_str = None

            result.append(
                {
                    "id": record.get("id"),
                    "newbuilding_id": nid,
                    "newbuilding_name": name_cache.get(nid, ""),
                    "date": date_str,
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
            )
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
