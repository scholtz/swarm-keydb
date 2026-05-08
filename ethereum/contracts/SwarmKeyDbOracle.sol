// SPDX-License-Identifier: MIT
pragma solidity ^0.8.24;

import "./ISwarmKeyDb.sol";

/// @title SwarmKeyDbOracle — Reference implementation of the SwarmKeyDb oracle pattern.
///
/// Allows Ethereum users to request reads/writes against a SwarmKeyDb instance via
/// on-chain events. An off-chain oracle (C# EthereumBridgeService) watches for these
/// events and fulfils requests by calling back into this contract.
///
/// @dev Security model
///   - Only the designated `oracle` address may call `dataWriteConfirmed` and
///     `dataReadFulfilled`.
///   - Any address may request a write or read.
///   - The contract owner may update the oracle address.
contract SwarmKeyDbOracle is ISwarmKeyDb {
    // ── State ─────────────────────────────────────────────────────────────────

    address public owner;

    /// @notice The oracle address permitted to deliver results.
    address public oracle;

    /// @notice Mapping from key → last confirmed Swarm hash written by the oracle.
    mapping(string => bytes32) public swarmHashes;

    /// @notice Mapping from key → last fulfilled value delivered by the oracle.
    mapping(string => bytes) public cachedValues;

    // ── Events ────────────────────────────────────────────────────────────────

    /// @dev Emitted when the oracle address is updated.
    event OracleUpdated(address indexed previousOracle, address indexed newOracle);

    /// @dev Emitted after the oracle confirms a write and records a Swarm hash.
    event WriteConfirmed(address indexed user, string key, bytes32 swarmHash);

    /// @dev Emitted after the oracle fulfils a read request.
    event ReadFulfilled(address indexed user, string key, bytes value);

    // ── Modifiers ─────────────────────────────────────────────────────────────

    modifier onlyOwner() {
        require(msg.sender == owner, "SwarmKeyDbOracle: caller is not the owner");
        _;
    }

    modifier onlyOracle() {
        require(msg.sender == oracle, "SwarmKeyDbOracle: caller is not the oracle");
        _;
    }

    // ── Constructor ───────────────────────────────────────────────────────────

    /// @param _oracle Initial oracle address (the off-chain C# service signer).
    constructor(address _oracle) {
        require(_oracle != address(0), "SwarmKeyDbOracle: oracle cannot be zero address");
        owner  = msg.sender;
        oracle = _oracle;
    }

    // ── Owner administration ─────────────────────────────────────────────────

    /// @notice Update the oracle address. Only the contract owner may call this.
    function setOracle(address _oracle) external onlyOwner {
        require(_oracle != address(0), "SwarmKeyDbOracle: oracle cannot be zero address");
        emit OracleUpdated(oracle, _oracle);
        oracle = _oracle;
    }

    // ── User-facing write/read requests ──────────────────────────────────────

    /// @notice Request that the oracle write `value` under `key` in SwarmKeyDb.
    ///         The off-chain service will emit a DataWriteConfirmed callback.
    /// @param key    SwarmKeyDb key (UTF-8).
    /// @param value  Payload to store (arbitrary bytes).
    function requestWrite(string calldata key, bytes calldata value) external {
        emit DataWriteRequested(msg.sender, key, value);
    }

    /// @notice Request that the oracle read the value at `key` from SwarmKeyDb.
    ///         The off-chain service will call back via dataReadFulfilled.
    /// @param key  SwarmKeyDb key (UTF-8).
    function requestRead(string calldata key) external {
        emit DataReadRequested(msg.sender, key);
    }

    // ── Oracle callbacks ──────────────────────────────────────────────────────

    /// @inheritdoc ISwarmKeyDb
    function dataWriteConfirmed(
        address user,
        string  calldata key,
        bytes32 swarmHash
    ) external onlyOracle override {
        swarmHashes[key] = swarmHash;
        emit WriteConfirmed(user, key, swarmHash);
    }

    /// @inheritdoc ISwarmKeyDb
    function dataReadFulfilled(
        address user,
        string  calldata key,
        bytes   calldata value
    ) external onlyOracle override {
        cachedValues[key] = value;
        emit ReadFulfilled(user, key, value);
    }

    // ── Convenience getters ───────────────────────────────────────────────────

    /// @notice Returns the Swarm hash last recorded for `key`, or bytes32(0) if none.
    function getSwarmHash(string calldata key) external view returns (bytes32) {
        return swarmHashes[key];
    }

    /// @notice Returns the last value delivered by the oracle for `key`.
    function getCachedValue(string calldata key) external view returns (bytes memory) {
        return cachedValues[key];
    }
}
