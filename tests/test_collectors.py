"""
Smoke tests for collector parsing logic (no real HTTP calls).
Run: pytest tests/ -v
"""
import pytest
from twstock.collectors.finmind import FinMindCollector, _MAJOR_TIERS, _RETAIL_TIERS


def test_major_tier_keywords_match():
    assert any("1,000,001" in kw for kw in _MAJOR_TIERS)


def test_retail_tier_keywords_match():
    assert any("1 to 999" in kw for kw in _RETAIL_TIERS)


def test_finmind_collector_builds_params_without_token():
    col = FinMindCollector(api_token="")
    params = col._params("TaiwanStockMonthRevenue", "2330", "2024-01-01")
    assert params["dataset"] == "TaiwanStockMonthRevenue"
    assert "token" not in params


def test_finmind_collector_builds_params_with_token():
    col = FinMindCollector(api_token="mytoken")
    params = col._params("TaiwanStockMonthRevenue", "2330", "2024-01-01")
    assert params["token"] == "mytoken"
