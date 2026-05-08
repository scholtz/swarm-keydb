// SPDX-License-Identifier: MIT
pragma solidity ^0.8.24;

/// @title ISwarmKeyDb — Standard interface for the SwarmKeyDb off-chain oracle pattern.
///
/// Smart contracts that want to read from or write to a SwarmKeyDb instance emit events
/// defined in this interface. An off-chain oracle (the C# EthereumBridgeService) listens
/// for these events and performs the corresponding SwarmKeyDb operations.
///
/// @dev Workflow
///   WRITE: Contract emits DataWriteRequested → Oracle writes (key, value) to SwarmKeyDb →
///          Oracle (optionally) calls DataWriteConfirmed to record the Swarm hash on-chain.
///   READ:  Contract emits DataReadRequested  → Oracle reads value from SwarmKeyDb →
///          Oracle calls DataReadFulfilled to deliver the value back on-chain.
interface ISwarmKeyDb {
    // ── Events emitted by the contract (picked up by the off-chain oracle) ───

    /// @notice Request the oracle to write `value` under `key` in the SwarmKeyDb store.
    /// @param user    The requesting address (used for access control / audit trail).
    /// @param key     The SwarmKeyDb key (UTF-8 string).
    /// @param value   The raw bytes payload to store.
    event DataWriteRequested(
        address indexed user,
        string key,
        bytes  value
    );

    /// @notice Request the oracle to read the value at `key` from the SwarmKeyDb store.
    ///         The oracle will call back via DataReadFulfilled.
    /// @param user    The requesting address.
    /// @param key     The SwarmKeyDb key.
    event DataReadRequested(
        address indexed user,
        string key
    );

    // ── Callbacks from the oracle back to the contract ───────────────────────

    /// @notice Called by the oracle after writing data to SwarmKeyDb.
    ///         `swarmHash` is the Swarm chunk hash (bzz reference) of the stored payload,
    ///         allowing any party to independently retrieve the data from the Swarm network.
    /// @param user       The address that originally requested the write.
    /// @param key        The key that was written.
    /// @param swarmHash  The Swarm content-address (bzz hash) of the stored payload.
    function dataWriteConfirmed(
        address user,
        string  calldata key,
        bytes32 swarmHash
    ) external;

    /// @notice Called by the oracle to deliver a value from SwarmKeyDb back on-chain.
    /// @param user   The address that originally requested the read.
    /// @param key    The key that was read.
    /// @param value  The raw bytes payload retrieved from SwarmKeyDb.
    function dataReadFulfilled(
        address user,
        string  calldata key,
        bytes   calldata value
    ) external;
}
