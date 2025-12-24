import { HardhatUserConfig } from "hardhat/config";
import "@nomicfoundation/hardhat-toolbox";
import * as dotenv from "dotenv";

dotenv.config();

const SEPOLIA_RPC_URL = process.env.SEPOLIA_RPC_URL || "";
const PRIVATE_KEY = process.env.PRIVATE_KEY || "";

const config: HardhatUserConfig = {
  solidity: "0.8.20",
  networks: {
    // In-process local network. Fork Sepolia when SEPOLIA_RPC_URL is set,
    // so your local runs use the same Uniswap v3 addresses as Sepolia.
    hardhat: SEPOLIA_RPC_URL
      ? {
          forking: { url: SEPOLIA_RPC_URL },
        }
      : {},

    // Only used if you run `npx hardhat node`
    localhost: {
      url: "http://127.0.0.1:8545",
    },

    // Real Sepolia testnet
    sepolia: {
      url: SEPOLIA_RPC_URL,
      accounts: PRIVATE_KEY ? [PRIVATE_KEY] : [],
    },
  },
};

export default config;
