# Data Dictionary

## Fact Tables

### fct_transactions
Central fact table at the individual transaction grain. Joins unified transactions from all 5 source systems to the golden customer dimension.

| Column | Type | Description |
|---|---|---|
| transaction_key | STRING | Surrogate key (PK) |
| source_transaction_id | STRING | Original ID from source system |
| customer_key | STRING | FK to dim_customers |
| date_key | DATE | FK to dim_date |
| transaction_type | STRING | invoice, payment_received, bank_transaction |
| transaction_category | STRING | accounts_receivable, cash_receipt, cash_disbursement |
| amount | NUMERIC | Transaction amount in base currency (USD) |
| cash_inflow | NUMERIC | Derived: inflow portion |
| cash_outflow | NUMERIC | Derived: outflow portion |
| revenue_amount | NUMERIC | Derived: revenue portion |
| source_system | STRING | quickbooks, stripe, netsuite, plaid |

### fct_revenue
Revenue fact combining recognized (accounting) and pipeline (Salesforce) revenue.

| Column | Type | Description |
|---|---|---|
| revenue_key | STRING | Surrogate key (PK) |
| customer_key | STRING | FK to dim_customers |
| date_key | DATE | Transaction/close date |
| revenue_stage | STRING | recognized, booked, pipeline, lost |
| amount | NUMERIC | Revenue amount in USD |
| source_system | STRING | Source of the revenue event |

### fct_cash_flow
Daily cash flow combining Plaid bank transactions and Stripe payouts.

### fct_accounts_receivable
AR aging with bucket classification (Current, 1-30, 31-60, 61-90, 90+ Days).

## Dimension Tables

### dim_customers
Entity-resolved golden customer record. Resolution priority: Salesforce > QuickBooks > Stripe.

| Column | Type | Description |
|---|---|---|
| customer_key | STRING | Golden surrogate key (PK) |
| stripe_customer_id | STRING | Source ID |
| quickbooks_customer_id | STRING | Source ID |
| netsuite_customer_id | STRING | Source ID |
| salesforce_account_id | STRING | Source ID |
| company_name | STRING | Best-available company name |
| email | STRING | Best-available email |
| customer_tier | STRING | Enterprise / Mid-Market / SMB / Starter / Prospect |
| lifetime_revenue | NUMERIC | Total recognized revenue |
| is_multi_source_match | BOOLEAN | True if matched across 2+ systems |

### dim_accounts
Unified chart of accounts with classification (Asset/Liability/Equity/Revenue/Expense).

### dim_date
Standard date dimension 2020-2030 with fiscal calendar and relative date references.

## Data Vault 2.0 Layer

### Hubs
| Table | Business Key | Sources |
|---|---|---|
| hub_customer | customer_bk + record_source | QuickBooks, Stripe, Salesforce |
| hub_account | account_bk + record_source | QuickBooks, NetSuite |
| hub_transaction | transaction_bk + record_source | All unified transactions |

### Links
| Table | Relationship |
|---|---|
| lnk_customer_account | Which accounts a customer is linked to |
| lnk_transaction_account | Which account a transaction posts to |

### Satellites
| Table | Tracks |
|---|---|
| sat_customer_details | QuickBooks customer attributes over time |
| sat_customer_stripe | Stripe-specific attributes + cross-system IDs |
| sat_account_balance | Account balance history |
| sat_transaction_details | Transaction attributes |

## Data Quality Tables

| Table | Purpose |
|---|---|
| dq_cross_source_reconciliation | Accounting vs bank balance by day |
| dq_golden_record_completeness | Completeness score per customer (0-100%) |
| dq_source_freshness | SLA compliance per source system |
| dq_transaction_anomalies | Z-score anomaly detection on daily volumes |
