"""
Plaid data extractor.

Pulls bank transactions, account metadata, and daily balances using
the Plaid API and loads them into BigQuery.
"""

import logging
import os
from datetime import datetime, timedelta

import plaid
from plaid.api import plaid_api
from plaid.model.transactions_get_request import TransactionsGetRequest
from plaid.model.transactions_get_request_options import TransactionsGetRequestOptions
from plaid.model.accounts_balance_get_request import AccountsBalanceGetRequest

from extractors.common import ExtractConfig, load_to_bigquery

logger = logging.getLogger(__name__)

PLAID_CLIENT_ID = os.environ["PLAID_CLIENT_ID"]
PLAID_SECRET = os.environ["PLAID_SECRET"]
PLAID_ENV = os.environ.get("PLAID_ENV", "production")
ACCESS_TOKENS = os.environ["PLAID_ACCESS_TOKENS"].split(",")
GCP_PROJECT = os.environ["GCP_PROJECT_ID"]
RAW_DATASET = os.environ.get("BQ_DATASET_RAW", "raw_plaid")

ENV_MAP = {
    "sandbox": plaid.Environment.Sandbox,
    "development": plaid.Environment.Development,
    "production": plaid.Environment.Production,
}


def get_plaid_client() -> plaid_api.PlaidApi:
    """Create Plaid API client."""
    configuration = plaid.Configuration(
        host=ENV_MAP[PLAID_ENV],
        api_key={"clientId": PLAID_CLIENT_ID, "secret": PLAID_SECRET},
    )
    api_client = plaid.ApiClient(configuration)
    return plaid_api.PlaidApi(api_client)


def extract_transactions(
    client: plaid_api.PlaidApi,
    access_token: str,
    start_date: datetime,
    end_date: datetime,
) -> list[dict]:
    """Extract all transactions for a linked account with pagination."""
    all_transactions = []
    offset = 0
    total = None

    while total is None or offset < total:
        request = TransactionsGetRequest(
            access_token=access_token,
            start_date=start_date.date(),
            end_date=end_date.date(),
            options=TransactionsGetRequestOptions(
                count=500,
                offset=offset,
            ),
        )
        response = client.transactions_get(request)
        transactions = [t.to_dict() for t in response.transactions]
        all_transactions.extend(transactions)

        total = response.total_transactions
        offset += len(transactions)
        logger.info(f"Fetched {offset}/{total} transactions")

    return all_transactions


def extract_balances(
    client: plaid_api.PlaidApi,
    access_token: str,
) -> list[dict]:
    """Get current balances for all accounts under an access token."""
    request = AccountsBalanceGetRequest(access_token=access_token)
    response = client.accounts_balance_get(request)

    balances = []
    for account in response.accounts:
        account_dict = account.to_dict()
        balance_record = {
            "account_id": account_dict["account_id"],
            "current": account_dict["balances"]["current"],
            "available": account_dict["balances"]["available"],
            "limit": account_dict["balances"]["limit"],
            "iso_currency_code": account_dict["balances"]["iso_currency_code"],
            "snapshot_date": datetime.utcnow().strftime("%Y-%m-%d"),
        }
        balances.append(balance_record)

    return balances


def run(lookback_days: int = 30):
    """Extract transactions and balances from all linked Plaid accounts."""
    client = get_plaid_client()
    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=lookback_days)

    all_transactions = []
    all_balances = []
    all_accounts = []

    for access_token in ACCESS_TOKENS:
        token = access_token.strip()
        logger.info(f"Processing Plaid access token: {token[:8]}...")

        # Transactions
        transactions = extract_transactions(client, token, start_date, end_date)
        all_transactions.extend(transactions)

        # Balances
        balances = extract_balances(client, token)
        all_balances.extend(balances)

    # Load transactions
    config = ExtractConfig(
        gcp_project=GCP_PROJECT,
        raw_dataset=RAW_DATASET,
        source_system="plaid",
        table_name="transactions",
    )
    load_to_bigquery(all_transactions, config)

    # Load balances
    config.table_name = "balances"
    config.write_disposition = "WRITE_APPEND"
    load_to_bigquery(all_balances, config)

    logger.info(
        f"Plaid extraction complete: {len(all_transactions)} transactions, "
        f"{len(all_balances)} balance snapshots"
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
