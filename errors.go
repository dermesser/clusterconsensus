package clusterconsensus

import "fmt"

const (
	// An error with the ConsensusClient
	ERR_CALL     string = "ERR_CALL"
	ERR_IO              = "ERR_IO"
	ERR_ENCODING        = "ERR_ENCODING"
	// We're currently in a bad state; try again later
	ERR_STATE    = "ERR_STATE"
	ERR_MAJORITY = "ERR_MAJORITY"
	ERR_DENIED   = "ERR_DENIED"
	ERR_GAP      = "ERR_GAP"
	ERR_CONNECT  = "ERR_CONNECT"
)

type ConsensusError struct {
	// one of ERR_*
	errEnum string
	// Description
	desc string
	// And/or
	inner error
}

func NewError(code, desc string, inner error) ConsensusError {
	return ConsensusError{errEnum: code, desc: desc, inner: inner}
}

func (e ConsensusError) Error() string {
	return fmt.Sprintf("%s: %s %s", e.errEnum, e.desc, e.inner.Error())
}

// Whether it makes sense to retry the operation later.
func (e ConsensusError) Retryable() bool {
	return e.errEnum == ERR_STATE || e.errEnum == ERR_CALL || e.errEnum == ERR_MAJORITY
}

// Returns an error code from ERR_*
func (e ConsensusError) Code() string {
	return e.errEnum
}
