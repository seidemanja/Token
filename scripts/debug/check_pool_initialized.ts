import { ethers } from "hardhat";

const { POOL_ADDRESS, POOL_TOKEN0, POOL_TOKEN1, network } = require("../env");

async function main() {
  console.log(`Checking pool (network=${network})`);
  const pool = await ethers.getContractAt("IUniswapV3PoolMinimal", POOL_ADDRESS);

  const token0 = (await pool.token0()).toLowerCase();
  const token1 = (await pool.token1()).toLowerCase();
  const slot0 = await pool.slot0();

  console.log("POOL_ADDRESS:", POOL_ADDRESS);
  console.log("token0 (chain):", token0);
  console.log("token1 (chain):", token1);
  console.log("token0 (env):  ", POOL_TOKEN0.toLowerCase());
  console.log("token1 (env):  ", POOL_TOKEN1.toLowerCase());
  console.log("slot0.sqrtPriceX96:", slot0.sqrtPriceX96.toString());

  if (slot0.sqrtPriceX96 === 0n) {
    throw new Error("Pool is NOT initialized (sqrtPriceX96 == 0). Re-run 02_create_and_init_pool.ts.");
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
