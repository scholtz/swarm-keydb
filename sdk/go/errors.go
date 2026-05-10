package swarmkeydb

import (
	"errors"
	"fmt"
)

// ConnectionError is returned when a connection to SwarmKeyDb fails.
type ConnectionError struct {
	Msg   string
	Cause error
}

func (e *ConnectionError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("swarmkeydb: connection error: %s: %v", e.Msg, e.Cause)
	}
	return fmt.Sprintf("swarmkeydb: connection error: %s", e.Msg)
}

func (e *ConnectionError) Unwrap() error { return e.Cause }

// CommandError is returned when the server replies with an error.
type CommandError struct {
	Command string
	Msg     string
}

func (e *CommandError) Error() string {
	return fmt.Sprintf("swarmkeydb: command %q error: %s", e.Command, e.Msg)
}

// TimeoutError is returned when a command exceeds its deadline.
type TimeoutError struct {
	Command string
}

func (e *TimeoutError) Error() string {
	return fmt.Sprintf("swarmkeydb: command %q timed out", e.Command)
}

// AuthError is returned when authentication fails.
type AuthError struct {
	Msg string
}

func (e *AuthError) Error() string {
	return fmt.Sprintf("swarmkeydb: auth error: %s", e.Msg)
}

// WatchConflictError is returned when a watched key is modified before EXEC.
var WatchConflictError = errors.New("swarmkeydb: watch conflict: transaction aborted")

// Nil is returned by StringCmd.Val when the key does not exist.
var Nil = errors.New("swarmkeydb: nil")
