import pytest
from src.ingest.main import _header_matches_contract, CONTRACT_COLUMNS


def test_schema_ok():
    header = ",".join(CONTRACT_COLUMNS)
    assert _header_matches_contract(header)


def test_schema_drift():
    bad_header = ",".join(CONTRACT_COLUMNS[:-1])  # missing last col
    with pytest.raises(ValueError):
        _header_matches_contract(bad_header)
