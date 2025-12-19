import { ethers } from "hardhat";

async function main() {
  const [deployer] = await ethers.getSigners();

  console.log("Deploying contracts with account:", deployer.address);
  
  const balance = await ethers.provider.getBalance(deployer.address);
  console.log("Account balance:", ethers.formatEther(balance), "ETH");

  const MyToken = await ethers.getContractFactory("MyToken");

  // 🔹 Constructor arguments
  const name = "Seideman Labs Token";
  const symbol = "SLT";
  const initialSupply = ethers.parseUnits("100000000", 18);

  const myToken = await MyToken.deploy(name, symbol, initialSupply);
  await myToken.waitForDeployment();
  
  console.log("MyToken deployed to:", await myToken.getAddress());
}

main()
  .then(() => process.exit(0))
  .catch((error) => {
    console.error("Deployment failed:", error);
    process.exit(1);
  });