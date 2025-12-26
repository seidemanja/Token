import { ethers } from "hardhat";
import { UNISWAP_SEPOLIA } from "../../config/uniswap.sepolia";

// Shared env resolver
const { network, TOKEN_ADDRESS } = require("../env");

const FEE = 3000;

async function main() {
  console.log(`Executing direct pool swap (network=${network}, signer=2)`);

  const tokenAddress = TOKEN_ADDRESS;
  if (!tokenAddress) throw new Error("TOKEN_ADDRESS not resolved");

  const signers = await ethers.getSigners();
  const trader = signers[2]; // <<< DIFFERENT WALLET

  const factory = await ethers.getContractAt(
    "IUniswapV3FactoryMinimal",
    UNISWAP_SEPOLIA.factory,
    trader
  );

  const poolAddr = await factory.getPool(tokenAddress, UNISWAP_SEPOLIA.weth, FEE);
  console.log("Pool:", poolAddr);

  const pool = await ethers.getContractAt(
    "IUniswapV3PoolMinimal",
    poolAddr,
    trader
  );

  const token0 = (await pool.token0()).toLowerCase();
  const WETH = UNISWAP_SEPOLIA.weth.toLowerCase();
  const zeroForOne = token0 === WETH;

  // Wrap WETH
  const weth = await ethers.getContractAt(
    "IWETH9Minimal",
    UNISWAP_SEPOLIA.weth,
    trader
  );

  const wethIn = ethers.parseEther("0.0001");
  await (await weth.deposit({ value: wethIn })).wait();

  // Deploy executor with payer = trader[2]
  const Exec = await ethers.getContractFactory(
    "PoolSwapExecutor",
    trader
  );
  const exec = await Exec.deploy(poolAddr, trader.address);
  await exec.waitForDeployment();

  // Approve executor
  const wethErc20 = await ethers.getContractAt(
    "@openzeppelin/contracts/token/ERC20/IERC20.sol:IERC20",
    UNISWAP_SEPOLIA.weth,
    trader
  );
  await (await wethErc20.approve(await exec.getAddress(), wethIn)).wait();

  // Swap bounds
  const MIN_SQRT_RATIO = 4295128739n;
  const MAX_SQRT_RATIO =
    1461446703485210103287273052203988822378723970342n;

  const sqrtLimit = zeroForOne
    ? MIN_SQRT_RATIO + 1n
    : MAX_SQRT_RATIO - 1n;

  const token = await ethers.getContractAt("MyToken", tokenAddress, trader);
  const balBefore = await token.balanceOf(trader.address);

  const tx = await exec.executeSwap(
    zeroForOne,
    BigInt(wethIn.toString()),
    sqrtLimit
  );
  const receipt = await tx.wait();

  const balAfter = await token.balanceOf(trader.address);

  console.log("Trader:", trader.address);
  console.log("Swap tx:", receipt?.hash);
  console.log("Token received:", (balAfter - balBefore).toString());
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
