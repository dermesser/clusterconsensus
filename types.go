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

// A remote member of the consensus
type Member struct {
	addr string
}

type InstanceNumber uint64
type SequenceNumber uint64

const (
	// Normal operation
	STATE_MASTER              int = iota
	STATE_PARTICIPANT_CLEAN       // from STATE_PARTICIPANT_PENDING; waiting for master requests
	STATE_PARTICIPANT_PENDING     // from STATE_PARTICIPANT_CLEAN; pending changes
	// During election
	STATE_CANDIDATE      // from STATE_PARTICIPANT_* or STATE_MASTER
	STATE_PENDING_MASTER // from STATE_PARTICIPANT_*; we have a staged master
)

// One participant of the consensus
// implements participating; implementation-provided
type Participant struct {
	members []Member
	master  map[InstanceNumber]Member // If a past Instance is attempted to be Prepare()d, then we can answer with the master of that Instance
	self    Member

	participants map[Member]ConsensusClient

	instance InstanceNumber // nth round
	serial   SequenceNumber // nth submission in this round

	state            State
	participantState int // See STATE_... constants

	stagedChanges map[SequenceNumber]Change // staging area for changes (i.e. temporary log)
	stagedMembers map[SequenceNumber]Change

	// Used to deserialize changes
	protoTypeDeserialize Change
}

// Implemented by Participant
// Used by Server for external requests calling into the participant, as well
// as making requests to remote participants.
type ParticipantStub interface {
	// Master -> participants
	Prepare(InstanceNumber, Member) (bool, error)

	// Master -> participants
	Accept(InstanceNumber, SequenceNumber, []Change) error
	// Master -> participants
	// Accept() with a different argument
	AddMember(InstanceNumber, SequenceNumber, Member) error
	// Master -> participants
	// Accept() with a different argument
	RemoveMember(InstanceNumber, SequenceNumber, Member) error

	// Master -> new participant
	StartParticipation(i InstanceNumber, s SequenceNumber, master Member, members []Member, snapshot []byte)

	// Participant -> master (so that non-masters can submit changes)
	Submit([]Change) error
}

type ConsensusClient interface {
	ParticipantStub
}

type ConsensusServer interface {
	ParticipantStub
}

// A component that receives external requests and calls a Participating component. Typically
// some RPC or HTTP server component that dispatches incoming requests to a participant.
type Server interface {
	Register(string, ConsensusServer) error
}
