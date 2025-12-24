// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

interface IUniswapV3FactoryMinimal {
  function createPool(address tokenA, address tokenB, uint24 fee) external returns (address pool);
  function getPool(address tokenA, address tokenB, uint24 fee) external view returns (address pool);
}
