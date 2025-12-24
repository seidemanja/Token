// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

interface IERC20Pay {
  function transferFrom(address from, address to, uint256 value) external returns (bool);
}

interface IUniswapV3SwapCallback {
  function uniswapV3SwapCallback(int256 amount0Delta, int256 amount1Delta, bytes calldata data) external;
}

interface IUniswapV3PoolSwap {
  function swap(
    address recipient,
    bool zeroForOne,
    int256 amountSpecified,
    uint160 sqrtPriceLimitX96,
    bytes calldata data
  ) external returns (int256 amount0, int256 amount1);

  function token0() external view returns (address);
  function token1() external view returns (address);
}

contract PoolSwapExecutor is IUniswapV3SwapCallback {
  address public immutable pool;
  address public immutable payer;

  constructor(address _pool, address _payer) {
    pool = _pool;
    payer = _payer;
  }

  // Call this from the payer EOA.
  function executeSwap(
    bool zeroForOne,
    int256 amountSpecified,
    uint160 sqrtPriceLimitX96
  ) external returns (int256 amount0, int256 amount1) {
    require(msg.sender == payer, "only payer");
    return IUniswapV3PoolSwap(pool).swap(payer, zeroForOne, amountSpecified, sqrtPriceLimitX96, bytes(""));
  }

  // Pool calls back here to collect the input token.
  function uniswapV3SwapCallback(int256 amount0Delta, int256 amount1Delta, bytes calldata) external override {
    require(msg.sender == pool, "bad callback");

    if (amount0Delta > 0) {
      IERC20Pay(IUniswapV3PoolSwap(pool).token0()).transferFrom(payer, pool, uint256(amount0Delta));
    }
    if (amount1Delta > 0) {
      IERC20Pay(IUniswapV3PoolSwap(pool).token1()).transferFrom(payer, pool, uint256(amount1Delta));
    }
  }
}
