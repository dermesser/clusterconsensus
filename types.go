package clusterconsensus

// A change that can be applied to a State and sent over the wire
// Client-provided
type Change interface {
	Serialize() []byte
	Deserialize([]byte) Change
}

// A state machine containing the overall state.
// Client-provided
type State interface {
	Snapshot() []byte
	Apply(Change)
	Install([]byte)
}

// A channel that is connected to a peer and can roundtrip messages.
type Transport interface {
	Send([]byte) error
	Recv() ([]byte, error)
}

// A remote member of the consensus
type Member struct {
	addr string
}

// One participant of the consensus
// implements participating; implementation-provided
type Participant struct {
	members []Member
	master  Member
	self    Member

	contacts map[Member]Transport

	instance uint64 // nth round
	serial   uint64 // nth submission in this round

	state   State
	staging map[uint64]Change // staging area for changes

	// Used to deserialize changes
	protoTypeDeserialize Change
}

// Implemented by Participant
// Used by Server for asynchronous, external callbacks
type Participating interface {
	Prepare(instance uint64, m Member) bool
	Accept(instance, serial uint64, changes []Change)
	StartParticipation(instance, serial uint64, master Member, members []Member, snapshot []byte)
}
