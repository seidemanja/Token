import { ethers } from "hardhat";
import fs from "fs";
import path from "path";

type Artifact = { abi: any[]; bytecode: string; linkReferences?: any };

function loadArtifact(relPath: string): Artifact {
  const fullPath = path.resolve(process.cwd(), relPath);
  const raw = fs.readFileSync(fullPath, "utf8");
  const json = JSON.parse(raw);
  return { abi: json.abi, bytecode: json.bytecode, linkReferences: json.linkReferences };
}

function linkLibrary(bytecode: string, linkRefs: any, libName: string, libAddress: string): string {
  if (!bytecode || !bytecode.startsWith("0x")) {
    throw new Error("Bytecode missing 0x prefix");
  }
  const addr = libAddress.replace(/^0x/, "");
  if (addr.length !== 40) {
    throw new Error(`Invalid library address: ${libAddress}`);
  }
  let linked = bytecode.slice(2);
  for (const file of Object.keys(linkRefs || {})) {
    const libs = linkRefs[file] || {};
    if (!libs[libName]) continue;
    for (const ref of libs[libName]) {
      const start = ref.start * 2;
      const length = ref.length * 2;
      linked = linked.slice(0, start) + addr + linked.slice(start + length);
    }
  }
  return "0x" + linked;
}

async function main() {
  const [deployer] = await ethers.getSigners();
  console.log("Deploying Uniswap V3 (local)");
  console.log("Deployer:", deployer.address);

  // Deploy local WETH9 (from our own contract)
  const WETH9 = await ethers.getContractFactory("WETH9");
  const weth = await WETH9.deploy();
  await weth.waitForDeployment();
  const wethAddr = await weth.getAddress();
  console.log("WETH9:", wethAddr);

  // Deploy Uniswap V3 Factory (artifact from node_modules)
  const factoryArtifact = loadArtifact(
    "node_modules/@uniswap/v3-core/artifacts/contracts/UniswapV3Factory.sol/UniswapV3Factory.json"
  );
  const Factory = new ethers.ContractFactory(
    factoryArtifact.abi,
    factoryArtifact.bytecode,
    deployer
  );
  const factory = await Factory.deploy();
  await factory.waitForDeployment();
  const factoryAddr = await factory.getAddress();
  console.log("UniswapV3Factory:", factoryAddr);

  // Deploy Position Descriptor (artifact from node_modules)
  // This bytecode links against NFTDescriptor, so deploy the library first.
  const nftDescriptorArtifact = loadArtifact(
    "node_modules/@uniswap/v3-periphery/artifacts/contracts/libraries/NFTDescriptor.sol/NFTDescriptor.json"
  );
  const NFTDescriptor = new ethers.ContractFactory(
    nftDescriptorArtifact.abi,
    nftDescriptorArtifact.bytecode,
    deployer
  );
  const nftDescriptor = await NFTDescriptor.deploy();
  await nftDescriptor.waitForDeployment();
  const nftDescriptorAddr = await nftDescriptor.getAddress();
  console.log("NFTDescriptor:", nftDescriptorAddr);

  const descriptorArtifact = loadArtifact(
    "node_modules/@uniswap/v3-periphery/artifacts/contracts/NonfungibleTokenPositionDescriptor.sol/NonfungibleTokenPositionDescriptor.json"
  );
  const linkedDescriptorBytecode = linkLibrary(
    descriptorArtifact.bytecode,
    descriptorArtifact.linkReferences,
    "NFTDescriptor",
    nftDescriptorAddr
  );
  const Descriptor = new ethers.ContractFactory(
    descriptorArtifact.abi,
    linkedDescriptorBytecode,
    deployer
  );
  const nativeLabel = ethers.encodeBytes32String("ETH");
  const descriptor = await Descriptor.deploy(wethAddr, nativeLabel);
  await descriptor.waitForDeployment();
  const descriptorAddr = await descriptor.getAddress();
  console.log("PositionDescriptor:", descriptorAddr);

  // Deploy NonfungiblePositionManager (artifact from node_modules)
  const npmArtifact = loadArtifact(
    "node_modules/@uniswap/v3-periphery/artifacts/contracts/NonfungiblePositionManager.sol/NonfungiblePositionManager.json"
  );
  const NPM = new ethers.ContractFactory(
    npmArtifact.abi,
    npmArtifact.bytecode,
    deployer
  );
  const npm = await NPM.deploy(factoryAddr, wethAddr, descriptorAddr);
  await npm.waitForDeployment();
  const npmAddr = await npm.getAddress();
  console.log("PositionManager:", npmAddr);

  console.log("\nAdd these to .env:");
  console.log(`LOCAL_UNISWAP_V3_FACTORY=${factoryAddr}`);
  console.log(`LOCAL_UNISWAP_V3_POSITION_MANAGER=${npmAddr}`);
  console.log(`LOCAL_WETH_ADDRESS=${wethAddr}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
