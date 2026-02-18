"""
Base API client with retry logic, rate limiting, and error handling.
"""

import logging
import time
from typing import Any, Optional

import requests
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

logger = logging.getLogger(__name__)


class RateLimitError(Exception):
    """Raised when API rate limit is hit."""
    pass


class APIClient:
    """
    Base API client with automatic retries and rate-limit handling.
    Subclass this for each source system.
    """

    def __init__(
        self,
        base_url: str,
        headers: Optional[dict] = None,
        max_retries: int = 5,
        requests_per_minute: int = 60,
    ):
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        if headers:
            self.session.headers.update(headers)
        self.max_retries = max_retries
        self.min_interval = 60.0 / requests_per_minute
        self._last_request_time = 0.0

    def _throttle(self):
        """Enforce minimum interval between requests."""
        elapsed = time.time() - self._last_request_time
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)
        self._last_request_time = time.time()

    @retry(
        retry=retry_if_exception_type((RateLimitError, requests.ConnectionError)),
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=60),
        before_sleep=lambda retry_state: logger.warning(
            f"Retry attempt {retry_state.attempt_number} after error"
        ),
    )
    def _request(
        self,
        method: str,
        endpoint: str,
        params: Optional[dict] = None,
        json_body: Optional[dict] = None,
    ) -> dict[str, Any]:
        """Make an HTTP request with retry logic."""
        self._throttle()
        url = f"{self.base_url}/{endpoint.lstrip('/')}"

        response = self.session.request(
            method=method,
            url=url,
            params=params,
            json=json_body,
            timeout=30,
        )

        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", 60))
            logger.warning(f"Rate limited. Waiting {retry_after}s")
            time.sleep(retry_after)
            raise RateLimitError("Rate limit exceeded")

        response.raise_for_status()
        return response.json()

    def get(self, endpoint: str, params: Optional[dict] = None) -> dict:
        return self._request("GET", endpoint, params=params)

    def post(self, endpoint: str, json_body: Optional[dict] = None) -> dict:
        return self._request("POST", endpoint, json_body=json_body)

    def get_paginated(
        self,
        endpoint: str,
        params: Optional[dict] = None,
        data_key: str = "data",
        next_key: str = "has_more",
        cursor_key: str = "starting_after",
        id_field: str = "id",
        max_pages: int = 1000,
    ) -> list[dict]:
        """
        Generic cursor-based pagination.
        Returns all records across all pages.
        """
        params = params or {}
        all_records = []

        for page in range(max_pages):
            response = self.get(endpoint, params=params)
            records = response.get(data_key, [])
            all_records.extend(records)

            if not response.get(next_key, False) or not records:
                break

            # Set cursor to last record's ID
            params[cursor_key] = records[-1][id_field]
            logger.info(f"Page {page + 1}: fetched {len(records)} records")

        logger.info(f"Total records fetched from {endpoint}: {len(all_records)}")
        return all_records
