const { ethers } = require("hardhat");

async function main() {
  const [deployer] = await ethers.getSigners();

  // Default oracle address — replace with your actual oracle signer address
  const oracleAddress = process.env.ORACLE_ADDRESS || deployer.address;

  console.log("Deploying SwarmKeyDbOracle...");
  console.log("  Deployer :", deployer.address);
  console.log("  Oracle   :", oracleAddress);

  const SwarmKeyDbOracle = await ethers.getContractFactory("SwarmKeyDbOracle");
  const contract = await SwarmKeyDbOracle.deploy(oracleAddress);
  await contract.waitForDeployment();

  const address = await contract.getAddress();
  console.log("SwarmKeyDbOracle deployed to:", address);
  console.log("");
  console.log("Set the following environment variables in your SwarmKeyDb server:");
  console.log(`  ETH_BRIDGE_ENABLED=true`);
  console.log(`  ETH_RPC_URL=<your-rpc-url>`);
  console.log(`  ETH_CONTRACT_ADDRESS=${address}`);
  console.log(`  ETH_PRIVATE_KEY=<oracle-private-key>`);
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
