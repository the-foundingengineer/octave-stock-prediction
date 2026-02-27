
# from typing import List


def calculate_growth(previous: float, current: float) -> float:
    if previous == 0:
        return 0.0

    growth = (current - previous) / previous

    # Floor at zero
    return max(growth, 0.0)


def forecast_next(current: float, growth: float) -> float:
    return current * (1 + growth)