import { ethers } from "hardhat";
import { UNISWAP_SEPOLIA } from "../../config/uniswap.sepolia";

// Shared env resolver (network-aware)
const {
  network,
  TOKEN_ADDRESS,
} = require("../env");

const FEE = 3000;

async function main() {
  console.log(`Executing direct pool swap (network=${network})`);

  const tokenAddress = TOKEN_ADDRESS;
  if (!tokenAddress) {
    throw new Error("TOKEN_ADDRESS not resolved from env.js");
  }

  const [trader] = await ethers.getSigners();

  const isLocal = network === "local";
  const factoryAddr = isLocal ? process.env.LOCAL_UNISWAP_V3_FACTORY : UNISWAP_SEPOLIA.factory;
  const wethAddr = isLocal ? process.env.LOCAL_WETH_ADDRESS : UNISWAP_SEPOLIA.weth;

  if (!factoryAddr) {
    throw new Error("Missing LOCAL_UNISWAP_V3_FACTORY in .env");
  }
  if (!wethAddr) {
    throw new Error("Missing LOCAL_WETH_ADDRESS in .env");
  }

  const factory = await ethers.getContractAt("IUniswapV3FactoryMinimal", factoryAddr);

  const poolAddr = await factory.getPool(
    tokenAddress,
    wethAddr,
    FEE
  );

  if (poolAddr === ethers.ZeroAddress) {
    throw new Error("Pool does not exist. Run 02_create_and_init_pool.ts first.");
  }

  console.log("Pool:", poolAddr);

  const pool = await ethers.getContractAt(
    "IUniswapV3PoolMinimal",
    poolAddr
  );

  const token0 = (await pool.token0()).toLowerCase();
  const WETH = wethAddr.toLowerCase();

  // We want WETH -> TOKEN
  // If token0 == WETH then zeroForOne = true (token0 -> token1)
  // Else zeroForOne = false (token1 -> token0)
  const zeroForOne = token0 === WETH;

  // Wrap ETH -> WETH
  const weth = await ethers.getContractAt(
    "IWETH9Minimal",
    wethAddr
  );

  const wethIn = ethers.parseEther("0.0001"); // small test amount
  await (await weth.deposit({ value: wethIn })).wait();

  // Deploy executor with (pool, payer)
  const Exec = await ethers.getContractFactory("PoolSwapExecutor");
  const exec = await Exec.deploy(poolAddr, trader.address);
  await exec.waitForDeployment();

  // Approve executor to pull WETH during callback
  const wethErc20 = await ethers.getContractAt(
    "@openzeppelin/contracts/token/ERC20/IERC20.sol:IERC20",
    wethAddr
  );

  await (await wethErc20.approve(await exec.getAddress(), wethIn)).wait();

  // Uniswap V3 price bounds
  const MIN_SQRT_RATIO = 4295128739n;
  const MAX_SQRT_RATIO =
    1461446703485210103287273052203988822378723970342n;

  const sqrtLimit = zeroForOne
    ? MIN_SQRT_RATIO + 1n
    : MAX_SQRT_RATIO - 1n;

  const token = await ethers.getContractAt("MyToken", tokenAddress);
  const balBefore = await token.balanceOf(trader.address);

  const tx = await exec.executeSwap(
    zeroForOne,
    BigInt(wethIn.toString()),
    sqrtLimit
  );

  const receipt = await tx.wait();
  const balAfter = await token.balanceOf(trader.address);

  console.log("Direct swap tx:", receipt?.hash);
  console.log("Token received:", (balAfter - balBefore).toString());
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
