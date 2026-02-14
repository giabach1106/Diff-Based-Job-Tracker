"""Airtable API client for listing job records."""

from __future__ import annotations

import logging
import re
import time
from typing import Any

import requests

from config import Settings

LOGGER = logging.getLogger(__name__)

AIRTABLE_URL_REGEX = re.compile(
    r"^https?://airtable\.com/(?P<base>app[a-zA-Z0-9]+)/(?P<share>shr[a-zA-Z0-9]+)/(?P<table>tbl[a-zA-Z0-9]+)"
)


class AirtableClient:
    """Client responsible for retrieving records from Airtable Web API."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {settings.airtable_pat}",
                "Content-Type": "application/json",
            }
        )
        self.base_url = "https://api.airtable.com/v0"

    @staticmethod
    def parse_identifiers_from_shared_url(url: str) -> tuple[str | None, str | None]:
        """Extract base and table identifiers from a shared Airtable URL."""

        match = AIRTABLE_URL_REGEX.match(url.strip())
        if not match:
            return None, None
        return match.group("base"), match.group("table")

    def list_records(self) -> list[dict[str, Any]]:
        """Return all records from configured table with pagination."""

        if not self.settings.airtable_base_id or not self.settings.airtable_table_id:
            raise ValueError("Airtable base/table id is missing.")

        endpoint = f"{self.base_url}/{self.settings.airtable_base_id}/{self.settings.airtable_table_id}"
        params: dict[str, Any] = {"pageSize": 100}
        if self.settings.airtable_view:
            params["view"] = self.settings.airtable_view

        all_records: list[dict[str, Any]] = []
        offset: str | None = None

        while True:
            page_params = dict(params)
            if offset:
                page_params["offset"] = offset

            payload = self._request_json(endpoint, params=page_params)
            records = payload.get("records", [])
            for record in records:
                if isinstance(record, dict):
                    all_records.append(record)

            offset_raw = payload.get("offset")
            if not isinstance(offset_raw, str) or not offset_raw:
                break
            offset = offset_raw

        return all_records

    def _request_json(
        self,
        url: str,
        params: dict[str, Any] | None = None,
        retries: int = 3,
    ) -> dict[str, Any]:
        """Perform a GET request with short exponential backoff retries."""

        last_error: Exception | None = None
        for attempt in range(retries):
            try:
                response = self.session.get(
                    url,
                    params=params,
                    timeout=self.settings.request_timeout_seconds,
                )
                if response.status_code in {429, 500, 502, 503, 504}:
                    raise requests.HTTPError(
                        f"Transient Airtable error: {response.status_code}",
                        response=response,
                    )
                response.raise_for_status()
                return response.json()
            except (requests.Timeout, requests.ConnectionError, requests.HTTPError) as exc:
                last_error = exc
                if attempt == retries - 1:
                    break
                sleep_seconds = 2**attempt
                LOGGER.warning("Airtable request failed (%s). Retrying in %ss.", exc, sleep_seconds)
                time.sleep(sleep_seconds)

        assert last_error is not None
        raise last_error
