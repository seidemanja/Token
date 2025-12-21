const hre = require("hardhat");

function clean(s) {
  return (s || "").trim().replace(/^"(.*)"$/, "$1").replace(/^'(.*)'$/, "$1");
}

async function main() {
  const contractAddress = clean(process.env.JSTVIP_ADDRESS);
  const newBaseURI = clean(process.env.NEW_BASE_URI);

  if (!contractAddress) throw new Error("Missing JSTVIP_ADDRESS in .env");
  if (!newBaseURI) throw new Error("Missing NEW_BASE_URI in env or .env");

  if (!hre.ethers.isAddress(contractAddress)) {
    throw new Error(`JSTVIP_ADDRESS is not a valid address: "${contractAddress}"`);
  }

  const nft = await hre.ethers.getContractAt("JSTVIP", contractAddress);

  const tx = await nft.setBaseURI(newBaseURI);
  console.log("setBaseURI tx:", tx.hash);

  await tx.wait();
  console.log("✅ Base URI updated to:", newBaseURI);
}

main().catch((e) => {
  console.error(e);
  process.exitCode = 1;
});
