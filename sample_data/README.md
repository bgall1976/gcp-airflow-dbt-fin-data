# Sample Data

Static JSON files that mimic real REST API responses from each source system. Use these to demo the pipeline without configuring API credentials.

## Files by Source

| Source | File | Mimics | Records |
|---|---|---|---|
| **Stripe** | `charges.json` | `GET /v1/charges` | 5 charges (1 refunded) |
| **Stripe** | `customers.json` | `GET /v1/customers` | 5 customers |
| **Stripe** | `balance_transactions.json` | `GET /v1/balance_transactions` | 5 txns (charges, refund, payout) |
| **QuickBooks** | `invoices.json` | `GET /v3/company/{id}/query` | 3 invoices |
| **QuickBooks** | `payments.json` | `GET /v3/company/{id}/query` | 1 payment linked to invoice |
| **QuickBooks** | `customers.json` | `GET /v3/company/{id}/query` | 3 customers |
| **QuickBooks** | `accounts.json` | `GET /v3/company/{id}/query` | 8 chart of accounts entries |
| **Salesforce** | `opportunities.json` | `GET /services/data/v59.0/query` | 5 opps (2 won, 1 lost, 2 pipeline) |
| **Salesforce** | `accounts.json` | `GET /services/data/v59.0/query` | 5 accounts |
| **NetSuite** | `journal_entries.json` | `GET /services/rest/record/v1/journalEntry` | 4 JEs with debit/credit lines |
| **NetSuite** | `accounts.json` | `GET /services/rest/record/v1/account` | 10 GL accounts |
| **Plaid** | `transactions.json` | `POST /transactions/get` | 6 bank transactions |
| **Plaid** | `balances.json` | `POST /accounts/balance/get` | 2 bank accounts |

## Cross-System Entity Map

The same 5 companies appear across multiple systems to demonstrate entity resolution in `int_golden_customers.sql`:

| Company | Stripe | QuickBooks | Salesforce | NetSuite | Plaid |
|---|---|---|---|---|---|
| Acme Corporation | `cus_stripe_001` | `QB-CUST-1001` | `001SF00000A001` | (via JE memo) | -- |
| Globex Industries | `cus_stripe_002` | `QB-CUST-1002` | `001SF00000A002` | -- | -- |
| Initech Solutions | `cus_stripe_003` | `QB-CUST-1003` | `001SF00000A003` | -- | -- |
| Umbrella Corp | `cus_stripe_004` | -- | `001SF00000A004` | -- | -- |
| Stark Technologies | `cus_stripe_005` | -- | `001SF00000A005` | -- | -- |

Cross-reference keys stored in Stripe customer `metadata` (e.g., `quickbooks_id`, `salesforce_id`) enable the golden record join.

## Financial Consistency

The numbers are internally consistent across systems:

- **Stripe charge** `ch_3Q1abc000000001` ($2,499) matches **QuickBooks invoice** `INV-1001` ($2,499) and **QuickBooks payment** `PMT-501` ($2,499)
- **Stripe payout** ($4,498 net after fees) matches the **Plaid deposit** `plaid_txn_001` ($2,426.53 net of the first charge)
- **NetSuite JE** `JE-2025-003` payroll of $134,562.50 matches the **Plaid transaction** `plaid_txn_002` (Gusto Payroll $134,562.50)
- **NetSuite JE** `JE-2025-002` AWS accrual of $8,750 matches **Plaid transaction** `plaid_txn_003` (AWS $8,750)

This cross-source consistency is what `dq_cross_source_reconciliation.sql` validates.

## Usage

To load this data into BigQuery as raw tables for the dbt pipeline:

```bash
# Load all JSON files into BigQuery raw datasets
for source in stripe quickbooks salesforce netsuite plaid; do
  for file in sample_data/${source}/*.json; do
    table_name=$(basename "$file" .json)
    bq load --source_format=NEWLINE_DELIMITED_JSON \
      --autodetect \
      raw_${source}.${table_name} \
      "$file"
  done
done
```

Or use the Python extractors in local/file mode:

```bash
python -m extractors.plaid.extract --source-file sample_data/plaid/transactions.json
python -m extractors.netsuite.extract --source-file sample_data/netsuite/journal_entries.json
```
