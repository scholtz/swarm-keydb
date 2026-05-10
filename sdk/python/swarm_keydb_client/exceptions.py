"""Custom exceptions for the swarm-keydb-client SDK."""

from __future__ import annotations


class SwarmKeyDbError(Exception):
    """Base exception for all SwarmKeyDb client errors."""


class SwarmKeyDbConnectionError(SwarmKeyDbError):
    """Raised when the client cannot connect to SwarmKeyDb.

    Check that the server is running and that the URL is correct.
    Default WebSocket port: 8765, HTTP port: 8080.
    """


class SwarmKeyDbCommandError(SwarmKeyDbError):
    """Raised when the server returns an error response for a command.

    The exception message includes the failing command and the server error text.
    """

    def __init__(self, command: str, server_error: str) -> None:
        self.command = command
        self.server_error = server_error
        super().__init__(f"Command '{command}' failed: {server_error}")


class SwarmKeyDbTimeoutError(SwarmKeyDbError):
    """Raised when a command does not receive a response within the timeout."""


class SwarmKeyDbAuthError(SwarmKeyDbConnectionError):
    """Raised when authentication fails."""
