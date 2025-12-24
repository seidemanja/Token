import { ethers } from "hardhat";
import { UNISWAP_SEPOLIA } from "./config/uniswap.sepolia";

async function main() {
  const routerAddr = UNISWAP_SEPOLIA.swapRouterV3;
  const [signer] = await ethers.getSigners();
  
  console.log("Router address:", routerAddr);
  
  // Check if contract exists
  const code = await ethers.provider.getCode(routerAddr);
  console.log("Contract code length:", code.length);
  console.log("Has code:", code !== "0x");
  
  if (code === "0x") {
    console.log("\n❌ Router contract does NOT exist at this address!");
    console.log("You need to deploy the SwapRouter contract.");
    return;
  }
  
  // Try calling factory
  try {
    const router = await ethers.getContractAt(
      ["function factory() external view returns (address)"],
      routerAddr
    );
    const factory = await router.factory();
    console.log("\nRouter's factory:", factory);
    console.log("Your config factory:", UNISWAP_SEPOLIA.factory);
    console.log("Match:", factory.toLowerCase() === UNISWAP_SEPOLIA.factory.toLowerCase());
  } catch (e: any) {
    console.log("\n⚠️  Router doesn't have factory() method:", e.message);
  }
  
  // Check WETH
  try {
    const router = await ethers.getContractAt(
      ["function WETH9() external view returns (address)"],
      routerAddr
    );
    const weth = await router.WETH9();
    console.log("\nRouter's WETH:", weth);
    console.log("Your config WETH:", UNISWAP_SEPOLIA.weth);
    console.log("Match:", weth.toLowerCase() === UNISWAP_SEPOLIA.weth.toLowerCase());
  } catch (e: any) {
    console.log("\n⚠️  Router doesn't have WETH9() method:", e.message);
  }
}

main().catch(console.error);
