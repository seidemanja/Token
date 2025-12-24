import { ethers } from "hardhat";
import { UNISWAP_SEPOLIA } from "../../config/uniswap.sepolia";

const FEE = 3000;

function sqrtBigInt(value: bigint): bigint {
  if (value < 0n) throw new Error("sqrt negative");
  if (value < 2n) return value;
  let x0 = value / 2n;
  let x1 = (x0 + value / x0) / 2n;
  while (x1 < x0) {
    x0 = x1;
    x1 = (x0 + value / x0) / 2n;
  }
  return x0;
}

function encodeSqrtPriceX96(amount1: bigint, amount0: bigint): bigint {
  const ratioX192 = (amount1 << 192n) / amount0;
  return sqrtBigInt(ratioX192);
}

async function main() {
  const tokenAddress = process.env.TOKEN_ADDRESS;
  if (!tokenAddress) throw new Error("Set TOKEN_ADDRESS env var");

  const factory = await ethers.getContractAt(
    "IUniswapV3FactoryMinimal",
    UNISWAP_SEPOLIA.factory
  );

  const weth = UNISWAP_SEPOLIA.weth;

  let poolAddress = await factory.getPool(tokenAddress, weth, FEE);
  if (poolAddress === ethers.ZeroAddress) {
    const tx = await factory.createPool(tokenAddress, weth, FEE);
    await tx.wait();
    poolAddress = await factory.getPool(tokenAddress, weth, FEE);
  }

  console.log("POOL_ADDRESS:", poolAddress);

  const pool = await ethers.getContractAt("IUniswapV3PoolMinimal", poolAddress);

  const token0 = (await pool.token0()).toLowerCase();
  const token1 = (await pool.token1()).toLowerCase();
  const TOKEN = tokenAddress.toLowerCase();
  const WETH = weth.toLowerCase();

  // Target: 1 TOKEN = 1e-7 WETH
  let amount0: bigint;
  let amount1: bigint;

  if (token0 === TOKEN && token1 === WETH) {
    // price = WETH/TOKEN = 1e-7
    amount0 = 10n ** 18n; // 1 TOKEN
    amount1 = 10n ** 11n; // 1e-7 WETH
  } else if (token0 === WETH && token1 === TOKEN) {
    // price = TOKEN/WETH = 1e7
    amount0 = 10n ** 18n; // 1 WETH
    amount1 = 10n ** 25n; // 1e7 TOKEN
  } else {
    throw new Error("Unexpected pool token ordering");
  }

  const sqrtPriceX96 = encodeSqrtPriceX96(amount1, amount0);

  const slot0 = await pool.slot0();
  if (slot0.sqrtPriceX96 !== 0n) {
    console.log("Already initialized. sqrtPriceX96:", slot0.sqrtPriceX96.toString());
    return;
  }

  const initTx = await pool.initialize(sqrtPriceX96);
  await initTx.wait();

  console.log("Initialized sqrtPriceX96:", sqrtPriceX96.toString());
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
