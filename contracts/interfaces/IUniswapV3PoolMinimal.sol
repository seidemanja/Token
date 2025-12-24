// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

interface IUniswapV3PoolMinimal {
  function token0() external view returns (address);
  function token1() external view returns (address);
  function initialize(uint160 sqrtPriceX96) external;

  function liquidity() external view returns (uint128);

  // slot0 returns multiple values; we only need sqrtPriceX96 and can ignore the rest.
  function slot0()
    external
    view
    returns (
      uint160 sqrtPriceX96,
      int24 tick,
      uint16 observationIndex,
      uint16 observationCardinality,
      uint16 observationCardinalityNext,
      uint8 feeProtocol,
      bool unlocked
    );
}
