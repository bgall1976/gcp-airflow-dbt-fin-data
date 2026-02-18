"""
Common utilities for loading extracted data into BigQuery raw tables.
All extractors write to a raw dataset with a _loaded_at timestamp column.
"""

import json
import logging
from datetime import datetime, timezone
from typing import Any

from google.cloud import bigquery
from google.cloud.bigquery import SchemaField, LoadJobConfig, WriteDisposition
from pydantic import BaseModel

logger = logging.getLogger(__name__)


class ExtractConfig(BaseModel):
    """Configuration for an extraction run."""
    gcp_project: str
    raw_dataset: str
    source_system: str
    table_name: str
    write_disposition: str = "WRITE_TRUNCATE"  # or WRITE_APPEND for incremental


def get_bq_client(project_id: str) -> bigquery.Client:
    """Create a BigQuery client."""
    return bigquery.Client(project=project_id)


def load_to_bigquery(
    records: list[dict[str, Any]],
    config: ExtractConfig,
) -> int:
    """
    Load a list of dictionaries into a BigQuery table.

    Adds a _loaded_at timestamp to each record. Uses schema auto-detection
    to handle evolving source schemas gracefully.

    Returns the number of rows loaded.
    """
    if not records:
        logger.warning(f"No records to load for {config.source_system}.{config.table_name}")
        return 0

    # Add extraction metadata
    loaded_at = datetime.now(timezone.utc).isoformat()
    for record in records:
        record["_loaded_at"] = loaded_at
        record["_source_system"] = config.source_system

    client = get_bq_client(config.gcp_project)
    table_ref = f"{config.gcp_project}.{config.raw_dataset}.{config.table_name}"

    job_config = LoadJobConfig(
        write_disposition=getattr(WriteDisposition, config.write_disposition),
        autodetect=True,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )

    # Convert to newline-delimited JSON
    ndjson = "\n".join(json.dumps(r, default=str) for r in records)

    load_job = client.load_table_from_json(
        records,
        table_ref,
        job_config=job_config,
    )
    load_job.result()  # Wait for completion

    logger.info(
        f"Loaded {load_job.output_rows} rows to {table_ref} "
        f"({config.write_disposition})"
    )
    return load_job.output_rows


def load_incremental(
    records: list[dict[str, Any]],
    config: ExtractConfig,
) -> int:
    """Load records incrementally (append mode)."""
    config.write_disposition = "WRITE_APPEND"
    return load_to_bigquery(records, config)
