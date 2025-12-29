/**
 * Fund controller signer on local Hardhat network.
 *
 * Uses Hardhat's default account #0 (rich account) to send ETH
 * to the controller signer address (JSTVIP_MINTER / PRIVATE_KEY address).
 */
require("dotenv").config();
const { ethers } = require("ethers");

async function main() {
  const rpcUrl = process.env.LOCAL_RPC_URL || "http://127.0.0.1:8545";
  const provider = new ethers.JsonRpcProvider(rpcUrl);

  // Hardhat local dev account #0 private key (known default)
  const hardhatPk =
    "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";
  const funder = new ethers.Wallet(hardhatPk, provider);

  // This is the signer your controller uses (derived from PRIVATE_KEY).
  // You can also set CONTROLLER_SIGNER_ADDRESS in .env if you want,
  // but here we use JSTVIP_MINTER if present, else infer from PRIVATE_KEY.
  const to =
    process.env.JSTVIP_MINTER ||
    new ethers.Wallet(
      process.env.PRIVATE_KEY?.startsWith("0x")
        ? process.env.PRIVATE_KEY
        : `0x${process.env.PRIVATE_KEY}`,
      provider
    ).address;

  const amountEth = process.env.FUND_CONTROLLER_ETH || "10";

  console.log("Funder:", funder.address);
  console.log("Funding controller signer:", to);
  console.log("Amount (ETH):", amountEth);

  const tx = await funder.sendTransaction({
    to,
    value: ethers.parseEther(amountEth),
  });

  console.log("Sent:", tx.hash);
  const rcpt = await tx.wait();
  console.log("Confirmed in block:", rcpt.blockNumber);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
