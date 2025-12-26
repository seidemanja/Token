import { ethers } from "hardhat";

// Shared env resolver (network-aware)
const { network } = require("./env");

async function main() {
  const [deployer] = await ethers.getSigners();

  console.log(`Deploying MyToken (network=${network})`);
  console.log("Deployer account:", deployer.address);

  const balance = await ethers.provider.getBalance(deployer.address);
  console.log("Account balance:", ethers.formatEther(balance), "ETH");

  const MyToken = await ethers.getContractFactory("MyToken");

  // Constructor arguments
  const name = "Seideman Labs Token";
  const symbol = "SLT";
  const initialSupply = ethers.parseUnits("100000000", 18);

  const myToken = await MyToken.deploy(name, symbol, initialSupply);
  await myToken.waitForDeployment();

  const tokenAddress = await myToken.getAddress();

  console.log("MyToken deployed to:", tokenAddress);
  console.log("");
  console.log("NEXT STEP:");
  console.log(
    `Add this to .env as ${
      network === "local" ? "LOCAL_TOKEN_ADDRESS" : "SEPOLIA_TOKEN_ADDRESS"
    }`
  );
  console.log(tokenAddress);
}

main()
  .then(() => process.exit(0))
  .catch((error) => {
    console.error("Deployment failed:", error);
    process.exit(1);
  });
