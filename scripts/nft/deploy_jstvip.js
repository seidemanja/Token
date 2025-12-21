const hre = require("hardhat");

async function main() {
  const name = process.env.JSTVIP_NAME || "JST_VIP";
  const symbol = process.env.JSTVIP_SYMBOL || "JST_VIP";
  const baseURI = process.env.JSTVIP_BASE_URI || "ipfs://REPLACE_ME/";

  const admin = process.env.JSTVIP_ADMIN;
  const minter = process.env.JSTVIP_MINTER;

  if (!admin || !minter) {
    throw new Error("Missing JSTVIP_ADMIN or JSTVIP_MINTER in .env");
  }

  const JSTVIP = await hre.ethers.getContractFactory("JSTVIP");
  const contract = await JSTVIP.deploy(name, symbol, baseURI, admin, minter);
  await contract.waitForDeployment();

  const addr = await contract.getAddress();
  console.log("JSTVIP deployed to:", addr);
  console.log("Params:", { name, symbol, baseURI, admin, minter });
}

main().catch((e) => {
  console.error(e);
  process.exitCode = 1;
});
