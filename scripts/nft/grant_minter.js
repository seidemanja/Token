const hre = require("hardhat");

async function main() {
  const nftAddress = process.env.JSTVIP_ADDRESS;
  const newMinter = process.env.NEW_MINTER;

  if (!nftAddress || !newMinter) {
    throw new Error("Set JSTVIP_ADDRESS and NEW_MINTER in .env");
  }

  if (!hre.ethers.isAddress(newMinter)) {
    throw new Error(`NEW_MINTER is not a valid address: "${newMinter}"`);
  }

  const nft = await hre.ethers.getContractAt("JSTVIP", nftAddress);

  const tx = await nft.setMinter(newMinter);
  await tx.wait();

  console.log("Granted MINTER_ROLE to:", newMinter);
}

main().catch((e) => {
  console.error(e);
  process.exitCode = 1;
});
