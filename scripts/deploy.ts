import { ethers } from "hardhat";

async function main() {
  const [deployer] = await ethers.getSigners();

  console.log("Deploying contracts with account:", deployer.address);
  
  const balance = await ethers.provider.getBalance(deployer.address);
  console.log("Account balance:", ethers.formatEther(balance), "ETH");

  const MyToken = await ethers.getContractFactory("MyToken");
  const myToken = await MyToken.deploy(ethers.parseUnits("1000000", 18));

  await myToken.waitForDeployment();
  const address = await myToken.getAddress();
  
  console.log("MyToken deployed to:", address);
}

main()
  .then(() => process.exit(0))
  .catch((error) => {
    console.error("Deployment failed:", error);
    process.exit(1);
  });