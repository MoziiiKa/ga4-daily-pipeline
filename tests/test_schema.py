"""Unit test: header validation helper.

We monkey-patch CONTRACT_COLUMNS so the test does not depend on
Cloud-Storage availability.
"""

import pytest
import importlib

import src.ingest.main as main_mod

# Monkey‑patch CONTRACT_COLUMNS to a deterministic list
main_mod.CONTRACT_COLUMNS = ["col_a", "col_b", "col_c"]
# Hot‑reload header matcher in case it captured old list
importlib.reload(main_mod)  # noqa: E402


def test_schema_ok():
    header = ",".join(main_mod.CONTRACT_COLUMNS)
    assert main_mod._header_matches_contract(header)


def test_schema_drift():
    bad_header = ",".join(main_mod.CONTRACT_COLUMNS[:-1])  # one missing
    with pytest.raises(ValueError):
        main_mod._header_matches_contract(bad_header)
