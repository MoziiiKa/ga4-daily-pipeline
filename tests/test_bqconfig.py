"""Unit test: BigQuery LoadJobConfig defaults"""

from google.cloud import bigquery
from src.ingest.bq_loader import _load_config


def test_load_job_flags():
    cfg = _load_config(autodetect=True)
    assert cfg.source_format == bigquery.SourceFormat.CSV
    assert cfg.allow_quoted_newlines is True
    assert cfg.write_disposition == bigquery.WriteDisposition.WRITE_APPEND
