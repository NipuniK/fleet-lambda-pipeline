def calculate_profitability(total_fare: float, fuel_cost: float, maintenance_cost: float) -> dict:
    """Pure function to calculate profitability metrics without Spark."""
    total_cost = fuel_cost + maintenance_cost
    net_profit = total_fare - total_cost
    return {
        "total_cost": total_cost,
        "net_profit": net_profit,
        "unprofitable": net_profit < 0
    }

def is_idle_threshold_exceeded(idle_seconds: float, threshold_seconds: int) -> bool:
    """Pure function to determine if idle duration exceeds threshold."""
    return idle_seconds > threshold_seconds
