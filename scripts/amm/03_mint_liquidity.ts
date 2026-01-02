import { ethers } from "hardhat";
import { UNISWAP_SEPOLIA } from "../../config/uniswap.sepolia";

// Shared env resolver (network-aware)
const {
  network,
  TOKEN_ADDRESS,
} = require("../env");

const FEE = 3000;
const TICK_LOWER = -887220;
const TICK_UPPER =  887220;

async function main() {
  console.log(`Minting liquidity (network=${network})`);

  const tokenAddress = TOKEN_ADDRESS;
  if (!tokenAddress) {
    throw new Error("TOKEN_ADDRESS not resolved from env.js");
  }

  const [deployer] = await ethers.getSigners();

  const token = await ethers.getContractAt("MyToken", tokenAddress);
  const weth  = await ethers.getContractAt(
    "IWETH9Minimal",
    UNISWAP_SEPOLIA.weth
  );
  const npm   = await ethers.getContractAt(
    "INonfungiblePositionManagerMinimal",
    UNISWAP_SEPOLIA.positionManager
  );

  // For fork: fine. For real Sepolia: also ok with faucet constraints.
  const wethToWrap   = ethers.parseEther("0.5");
  // const tokenAmount  = ethers.parseUnits("200000", 18);
  const tokenAmount = ethers.parseUnits("5000000", 18);

  // Wrap ETH → WETH
  await (await weth.deposit({ value: wethToWrap })).wait();

  // Approvals
  await (await token.approve(await npm.getAddress(), tokenAmount)).wait();
  await (await weth.approve(await npm.getAddress(), wethToWrap)).wait();

  // Token ordering must match Uniswap V3 rules
  const token0 =
    tokenAddress.toLowerCase() < UNISWAP_SEPOLIA.weth.toLowerCase()
      ? tokenAddress
      : UNISWAP_SEPOLIA.weth;

  const token1 =
    token0 === tokenAddress ? UNISWAP_SEPOLIA.weth : tokenAddress;

  const amount0Desired =
    token0 === tokenAddress ? tokenAmount : wethToWrap;

  const amount1Desired =
    token1 === tokenAddress ? tokenAmount : wethToWrap;

  const params = {
    token0,
    token1,
    fee: FEE,
    tickLower: TICK_LOWER,
    tickUpper: TICK_UPPER,
    amount0Desired,
    amount1Desired,
    amount0Min: 0,
    amount1Min: 0,
    recipient: deployer.address,
    deadline: ((await ethers.provider.getBlock("latest"))?.timestamp ?? Math.floor(Date.now() / 1000)) + 600,
  };

  await npm.mint.staticCall(params);

  const tx = await npm.mint(params);
  const receipt = await tx.wait();

  console.log("Liquidity minted. Tx:", receipt?.hash);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
