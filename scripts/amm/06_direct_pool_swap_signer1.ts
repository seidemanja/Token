import { ethers } from "hardhat";
import { UNISWAP_SEPOLIA } from "../../config/uniswap.sepolia";

// Shared env resolver (network-aware)
const { network, TOKEN_ADDRESS } = require("../env");

const FEE = 3000;

async function main() {
  console.log(`Executing direct pool swap (network=${network})`);

  const tokenAddress = TOKEN_ADDRESS;
  if (!tokenAddress) throw new Error("TOKEN_ADDRESS not resolved from env.js");

  const signers = await ethers.getSigners();
  const trader = signers[1]; // <-- use signer[1] end-to-end

  // IMPORTANT: all contract interactions below are connected to trader
  const factory = await ethers.getContractAt(
    "IUniswapV3FactoryMinimal",
    UNISWAP_SEPOLIA.factory,
    trader
  );

  const poolAddr = await factory.getPool(tokenAddress, UNISWAP_SEPOLIA.weth, FEE);
  console.log("Pool:", poolAddr);

  const pool = await ethers.getContractAt("IUniswapV3PoolMinimal", poolAddr, trader);
  const token0 = (await pool.token0()).toLowerCase();
  const WETH = UNISWAP_SEPOLIA.weth.toLowerCase();

  // We want WETH -> TOKEN.
  // If token0 == WETH then zeroForOne = true (token0->token1).
  // Else zeroForOne = false (token1->token0).
  const zeroForOne = token0 === WETH;

  // Wrap ETH -> WETH as trader[1]
  const weth = await ethers.getContractAt("IWETH9Minimal", UNISWAP_SEPOLIA.weth, trader);
  const wethIn = ethers.parseEther("0.0001");
  await (await weth.deposit({ value: wethIn })).wait();

  // Deploy executor with payer = trader[1]
  const Exec = await ethers.getContractFactory("PoolSwapExecutor", trader);
  const exec = await Exec.deploy(poolAddr, trader.address);
  await exec.waitForDeployment();

  // Approve executor to pull WETH from trader[1] during callback
  const wethErc20 = await ethers.getContractAt(
    "@openzeppelin/contracts/token/ERC20/IERC20.sol:IERC20",
    UNISWAP_SEPOLIA.weth,
    trader
  );
  await (await wethErc20.approve(await exec.getAddress(), wethIn)).wait();

  // Price limit bounds (Uniswap v3 constants)
  const MIN_SQRT_RATIO = 4295128739n;
  const MAX_SQRT_RATIO = 1461446703485210103287273052203988822378723970342n;
  const sqrtLimit = zeroForOne ? (MIN_SQRT_RATIO + 1n) : (MAX_SQRT_RATIO - 1n);

  const token = await ethers.getContractAt("MyToken", tokenAddress, trader);
  const balBefore = await token.balanceOf(trader.address);

  // Execute swap as trader[1]
  const tx = await exec.executeSwap(zeroForOne, BigInt(wethIn.toString()), sqrtLimit);
  const receipt = await tx.wait();

  const balAfter = await token.balanceOf(trader.address);

  console.log("Trader:", trader.address);
  console.log("Direct swap tx:", receipt?.hash);
  console.log("Token received:", (balAfter - balBefore).toString());
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
