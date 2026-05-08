const { expect } = require("chai");
const { ethers } = require("hardhat");

describe("SwarmKeyDbOracle", function () {
  let oracle;
  let owner;
  let oracleAccount;
  let user;

  beforeEach(async function () {
    [owner, oracleAccount, user] = await ethers.getSigners();

    const SwarmKeyDbOracle = await ethers.getContractFactory("SwarmKeyDbOracle");
    oracle = await SwarmKeyDbOracle.deploy(oracleAccount.address);
    await oracle.waitForDeployment();
  });

  // ── Deployment ─────────────────────────────────────────────────────────────

  it("sets the correct owner and oracle on deployment", async function () {
    expect(await oracle.owner()).to.equal(owner.address);
    expect(await oracle.oracle()).to.equal(oracleAccount.address);
  });

  it("reverts when deployed with zero-address oracle", async function () {
    const SwarmKeyDbOracle = await ethers.getContractFactory("SwarmKeyDbOracle");
    await expect(
      SwarmKeyDbOracle.deploy(ethers.ZeroAddress)
    ).to.be.revertedWith("SwarmKeyDbOracle: oracle cannot be zero address");
  });

  // ── setOracle ──────────────────────────────────────────────────────────────

  it("allows the owner to update the oracle address", async function () {
    await expect(oracle.connect(owner).setOracle(user.address))
      .to.emit(oracle, "OracleUpdated")
      .withArgs(oracleAccount.address, user.address);

    expect(await oracle.oracle()).to.equal(user.address);
  });

  it("reverts when a non-owner tries to update the oracle address", async function () {
    await expect(
      oracle.connect(user).setOracle(user.address)
    ).to.be.revertedWith("SwarmKeyDbOracle: caller is not the owner");
  });

  it("reverts when setting oracle to zero address", async function () {
    await expect(
      oracle.connect(owner).setOracle(ethers.ZeroAddress)
    ).to.be.revertedWith("SwarmKeyDbOracle: oracle cannot be zero address");
  });

  // ── requestWrite ───────────────────────────────────────────────────────────

  it("emits DataWriteRequested when requestWrite is called", async function () {
    const key = "profile:alice";
    const value = ethers.toUtf8Bytes("Hello, Swarm!");

    await expect(oracle.connect(user).requestWrite(key, value))
      .to.emit(oracle, "DataWriteRequested")
      .withArgs(user.address, key, value);
  });

  it("emits DataWriteRequested with arbitrary binary value", async function () {
    const key = "avatar:alice";
    const value = new Uint8Array([0x00, 0x01, 0x02, 0xff]);

    await expect(oracle.connect(user).requestWrite(key, value))
      .to.emit(oracle, "DataWriteRequested")
      .withArgs(user.address, key, value);
  });

  // ── requestRead ────────────────────────────────────────────────────────────

  it("emits DataReadRequested when requestRead is called", async function () {
    const key = "profile:alice";

    await expect(oracle.connect(user).requestRead(key))
      .to.emit(oracle, "DataReadRequested")
      .withArgs(user.address, key);
  });

  // ── dataWriteConfirmed ─────────────────────────────────────────────────────

  it("oracle can confirm a write and updates the stored Swarm hash", async function () {
    const key = "profile:alice";
    const swarmHash = ethers.randomBytes(32);
    const swarmHash32 = ethers.hexlify(swarmHash);

    await expect(
      oracle.connect(oracleAccount).dataWriteConfirmed(user.address, key, swarmHash32)
    )
      .to.emit(oracle, "WriteConfirmed")
      .withArgs(user.address, key, swarmHash32);

    expect(await oracle.getSwarmHash(key)).to.equal(swarmHash32);
  });

  it("reverts when a non-oracle tries to confirm a write", async function () {
    const swarmHash = ethers.randomBytes(32);
    await expect(
      oracle.connect(user).dataWriteConfirmed(user.address, "k", ethers.hexlify(swarmHash))
    ).to.be.revertedWith("SwarmKeyDbOracle: caller is not the oracle");
  });

  // ── dataReadFulfilled ─────────────────────────────────────────────────────

  it("oracle can fulfil a read request and caches the value on-chain", async function () {
    const key = "profile:alice";
    const value = ethers.toUtf8Bytes("Alice in Swarm-land");

    await expect(
      oracle.connect(oracleAccount).dataReadFulfilled(user.address, key, value)
    )
      .to.emit(oracle, "ReadFulfilled")
      .withArgs(user.address, key, value);

    const cached = await oracle.getCachedValue(key);
    expect(ethers.toUtf8String(cached)).to.equal("Alice in Swarm-land");
  });

  it("reverts when a non-oracle tries to fulfil a read", async function () {
    await expect(
      oracle.connect(user).dataReadFulfilled(user.address, "k", ethers.toUtf8Bytes("v"))
    ).to.be.revertedWith("SwarmKeyDbOracle: caller is not the oracle");
  });

  // ── Full oracle round-trip ─────────────────────────────────────────────────

  it("full round-trip: user requests write, oracle confirms with Swarm hash", async function () {
    const key = "dao:proposal:1";
    const value = ethers.toUtf8Bytes(JSON.stringify({ title: "Fund dev", amount: "100 ETH" }));
    const swarmHash = ethers.hexlify(ethers.randomBytes(32));

    // 1. User triggers a write request
    const writeTx = await oracle.connect(user).requestWrite(key, value);
    const writeReceipt = await writeTx.wait();
    const writeEvent = writeReceipt.logs.find(
      (l) => l.fragment?.name === "DataWriteRequested"
    );
    expect(writeEvent).to.not.be.undefined;
    expect(writeEvent.args.user).to.equal(user.address);
    expect(writeEvent.args.key).to.equal(key);

    // 2. Off-chain oracle processes the event, writes to SwarmKeyDb, obtains Swarm hash,
    //    then calls back on-chain.
    await oracle.connect(oracleAccount).dataWriteConfirmed(user.address, key, swarmHash);

    // 3. Anyone can verify the Swarm hash on-chain.
    expect(await oracle.getSwarmHash(key)).to.equal(swarmHash);
  });

  it("full round-trip: user requests read, oracle fulfils with cached value", async function () {
    const key = "nft:metadata:42";
    const value = ethers.toUtf8Bytes('{"name":"Galaxy #42","image":"bzz://abc123"}');

    // 1. User requests a read
    await oracle.connect(user).requestRead(key);

    // 2. Oracle reads from SwarmKeyDb and delivers the value on-chain
    await oracle.connect(oracleAccount).dataReadFulfilled(user.address, key, value);

    // 3. Contract now has the cached value available
    const cached = await oracle.getCachedValue(key);
    expect(ethers.toUtf8String(cached)).to.equal('{"name":"Galaxy #42","image":"bzz://abc123"}');
  });
});
