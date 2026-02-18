"""
NetSuite data extractor.

Uses the SuiteTalk REST API with token-based authentication to pull
financial data from NetSuite and load it into BigQuery.
"""

import hashlib
import hmac
import logging
import os
import time
import urllib.parse
from base64 import b64encode
from uuid import uuid4

from extractors.common import APIClient, ExtractConfig, load_to_bigquery

logger = logging.getLogger(__name__)

NS_ACCOUNT_ID = os.environ["NS_ACCOUNT_ID"]
NS_CONSUMER_KEY = os.environ["NS_CONSUMER_KEY"]
NS_CONSUMER_SECRET = os.environ["NS_CONSUMER_SECRET"]
NS_TOKEN_ID = os.environ["NS_TOKEN_ID"]
NS_TOKEN_SECRET = os.environ["NS_TOKEN_SECRET"]
GCP_PROJECT = os.environ["GCP_PROJECT_ID"]
RAW_DATASET = os.environ.get("BQ_DATASET_RAW", "raw_netsuite")

SUITEQL_QUERIES = {
    "transactions": """
        SELECT t.id, t.tranId AS tran_id, t.type, t.status, t.entity,
               t.subsidiary, t.department, t.currency, t.exchangeRate AS exchange_rate,
               t.total, t.tranDate AS tran_date, t.dueDate AS due_date,
               t.posting, t.voided, t.memo,
               t.dateCreated AS date_created, t.lastModifiedDate AS last_modified_date
        FROM transaction t
        WHERE t.tranDate >= '1/1/2020'
    """,
    "transaction_lines": """
        SELECT tl.transaction, tl.lineSequenceNumber, tl.account, tl.amount,
               tl.debit, tl.credit, tl.department, tl.class, tl.location, tl.memo
        FROM transactionLine tl
        INNER JOIN transaction t ON tl.transaction = t.id
        WHERE t.tranDate >= '1/1/2020'
    """,
    "accounts": """
        SELECT a.id, a.acctName AS acct_name, a.acctNumber AS acct_number,
               a.acctType AS acct_type, a.generalRateType AS general_rate_type,
               a.parent, a.isInactive AS is_inactive
        FROM account a
    """,
    "vendors": """
        SELECT v.id, v.entityId AS entity_id, v.companyName AS company_name,
               v.email, v.phone, v.isInactive AS is_inactive,
               v.dateCreated AS date_created
        FROM vendor v
    """,
    "customers": """
        SELECT c.id, c.entityId AS entity_id, c.companyName AS company_name,
               c.email, c.phone, c.isInactive AS is_inactive,
               c.dateCreated AS date_created
        FROM customer c
    """,
    "subsidiaries": """
        SELECT s.id, s.name, s.country, s.currency, s.isInactive AS is_inactive
        FROM subsidiary s
    """,
    "departments": """
        SELECT d.id, d.name, d.parent, d.isInactive AS is_inactive
        FROM department d
    """,
}


class NetSuiteClient(APIClient):
    """NetSuite REST API client with OAuth 1.0 TBA authentication."""

    def __init__(self):
        account_slug = NS_ACCOUNT_ID.replace("_", "-").lower()
        base_url = f"https://{account_slug}.suitetalk.api.netsuite.com/services/rest"
        super().__init__(base_url=base_url, requests_per_minute=20)

    def _generate_oauth_header(self, method: str, url: str) -> str:
        """Generate OAuth 1.0 authorization header for TBA."""
        nonce = uuid4().hex
        timestamp = str(int(time.time()))

        params = {
            "oauth_consumer_key": NS_CONSUMER_KEY,
            "oauth_nonce": nonce,
            "oauth_signature_method": "HMAC-SHA256",
            "oauth_timestamp": timestamp,
            "oauth_token": NS_TOKEN_ID,
            "oauth_version": "1.0",
        }

        # Build signature base string
        sorted_params = "&".join(
            f"{urllib.parse.quote(k, safe='')}={urllib.parse.quote(v, safe='')}"
            for k, v in sorted(params.items())
        )
        base_string = f"{method.upper()}&{urllib.parse.quote(url, safe='')}&{urllib.parse.quote(sorted_params, safe='')}"

        # Sign with consumer + token secrets
        signing_key = f"{urllib.parse.quote(NS_CONSUMER_SECRET, safe='')}&{urllib.parse.quote(NS_TOKEN_SECRET, safe='')}"
        signature = b64encode(
            hmac.new(
                signing_key.encode(),
                base_string.encode(),
                hashlib.sha256,
            ).digest()
        ).decode()

        params["oauth_signature"] = signature
        realm = NS_ACCOUNT_ID.upper()

        header_parts = [f'{k}="{urllib.parse.quote(v, safe="")}"' for k, v in params.items()]
        return f'OAuth realm="{realm}", ' + ", ".join(header_parts)

    def suiteql(self, query: str) -> list[dict]:
        """Execute a SuiteQL query with pagination."""
        all_records = []
        offset = 0
        limit = 1000
        url = f"{self.base_url}/record/v1/suiteql"

        while True:
            self.session.headers["Authorization"] = self._generate_oauth_header("POST", url)
            self.session.headers["Prefer"] = "transient"

            response = self.post(
                "record/v1/suiteql",
                json_body={"q": f"{query} OFFSET {offset} FETCH NEXT {limit} ROWS ONLY"},
            )

            items = response.get("items", [])
            all_records.extend(items)

            if not response.get("hasMore", False):
                break

            offset += limit
            logger.info(f"SuiteQL pagination: {len(all_records)} records fetched")

        return all_records


def run():
    """Extract all configured NetSuite entities."""
    client = NetSuiteClient()

    for table_name, query in SUITEQL_QUERIES.items():
        logger.info(f"Extracting NetSuite {table_name}...")

        records = client.suiteql(query)

        config = ExtractConfig(
            gcp_project=GCP_PROJECT,
            raw_dataset=RAW_DATASET,
            source_system="netsuite",
            table_name=table_name,
        )

        rows_loaded = load_to_bigquery(records, config)
        logger.info(f"NetSuite {table_name}: {rows_loaded} rows loaded")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
