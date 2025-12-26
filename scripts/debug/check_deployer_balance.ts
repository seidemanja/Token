import { ethers } from "hardhat";

async function main() {
  const [deployer] = await ethers.getSigners();
  const tokenAddr = process.env.LOCAL_TOKEN_ADDRESS;

  if (!tokenAddr) {
    throw new Error("LOCAL_TOKEN_ADDRESS not set");
  }

  const token = await ethers.getContractAt(
    ["function balanceOf(address) view returns (uint256)"],
    tokenAddr
  );

  const bal = await token.balanceOf(deployer.address);

  console.log("Deployer address:", deployer.address);
  console.log("Deployer token balance:", ethers.formatUnits(bal, 18));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
