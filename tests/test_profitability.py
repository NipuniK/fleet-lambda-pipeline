import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "common"))
from business_logic import calculate_profitability

def test_calculate_profitability_profitable():
    result = calculate_profitability(total_fare=100.0, fuel_cost=20.0, maintenance_cost=10.0)
    assert result["total_cost"] == 30.0
    assert result["net_profit"] == 70.0
    assert result["unprofitable"] is False

def test_calculate_profitability_unprofitable():
    result = calculate_profitability(total_fare=50.0, fuel_cost=40.0, maintenance_cost=20.0)
    assert result["total_cost"] == 60.0
    assert result["net_profit"] == -10.0
    assert result["unprofitable"] is True
