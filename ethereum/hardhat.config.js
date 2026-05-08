require("@nomicfoundation/hardhat-toolbox");

/** @type import('hardhat/config').HardhatUserConfig */
module.exports = {
  solidity: "0.8.24",
  networks: {
    // Local Hardhat node (for development and CI)
    localhost: {
      url: "http://127.0.0.1:8545",
    },
    // Sepolia testnet — configure via environment variables
    sepolia: {
      url: process.env.ETH_RPC_URL || "",
      accounts: process.env.ETH_PRIVATE_KEY ? [process.env.ETH_PRIVATE_KEY] : [],
    },
  },
};
