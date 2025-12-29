"""
sim/price.py

Utilities for converting Uniswap V3 sqrtPriceX96 to a human-readable price.

Uniswap V3:
  price(token1/token0) = (sqrtPriceX96 / 2**96)^2

If your TOKEN is token0 and WETH is token1, then:
  price_weth_per_token = token1/token0

If your TOKEN is token1, then invert accordingly.

We store:
- price_weth_per_token (float)
- normalized_price (price / first_price_in_range)
"""

from __future__ import annotations

from dataclasses import dataclass


Q96 = 2 ** 96


def sqrt_price_x96_to_price_token1_per_token0(sqrt_price_x96: int) -> float:
    """Convert sqrtPriceX96 to price = token1/token0."""
    sp = sqrt_price_x96 / Q96
    return sp * sp


@dataclass(frozen=True)
class PriceResult:
    price_weth_per_token: float
    normalized_price: float
