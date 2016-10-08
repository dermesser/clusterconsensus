package clusterconsensus

import (
	"io"
	"sync"
)

type InstanceNumber uint64
type SequenceNumber uint64

const (
	// Normal operation
	state_MASTER int = iota
	state_UNJOINED
	state_PARTICIPANT_CLEAN   // from state_PARTICIPANT_PENDING; waiting for master requests
	state_PARTICIPANT_PENDING // from state_PARTICIPANT_CLEAN; pending changes
	state_REMOVED             // Removed from cluster.
	// During election
	state_CANDIDATE      // from state_PARTICIPANT_* or state_MASTER
	state_PENDING_MASTER // from state_PARTICIPANT_*; we have a staged master
)

// Factory for connections to remote participants
type Connector interface {
	// Connect to member m in cluster c
	Connect(c string, m Member) (ConsensusClient, error)
}

// A change that can be applied to a State and sent over the wire
// Client-provided; can be any type
type Change interface {
	Serialize() []byte
}

// Deserialzie a Change from a bytestring.
type ChangeDeserializer interface {
	Deserialize([]byte) Change
}

// A state machine containing the overall state.
// Client-provided; can be any type
type State interface {
	Snapshot() []byte
	Apply(Change)
	Install([]byte)
}

// A remote member of the consensus
type Member struct {
	Address string
}

// One participant of the consensus
// Implements ConsensusServer
type Participant struct {
	sync.Mutex

	cluster string
	members []Member
	master  map[InstanceNumber]Member // If a past Instance is attempted to be Prepare()d, then we can answer with the master of that Instance
	self    Member

	participants map[Member]ConsensusClient

	instance InstanceNumber // nth round
	sequence SequenceNumber // nth submission in this round

	state            State
	participantState int // See state_... constants

	stagedChanges  map[SequenceNumber][]Change // staging area for changes (i.e. temporary log)
	stagedMembers  map[SequenceNumber]Member
	stagedRemovals map[SequenceNumber]Member

	connector Connector
}

// Implemented by Participant
// Used by Server for external requests calling into the participant, as well
// as making requests to remote participants.
type ParticipantStub interface {
	// Master -> participants; instance number must be greater than any one previously used;
	// second argument is the sending member (i.e. master)
	// The return value is the highest instance number (equal to argument means positive vote, greater than
	// argument means that vote has not been given).
	Prepare(InstanceNumber, Member) (InstanceNumber, error)

	// Master -> participants
	Accept(InstanceNumber, SequenceNumber, []Change) (bool, error)
	// Master -> participants
	// Accept() with a different argument
	AddMember(InstanceNumber, SequenceNumber, Member) error
	// Master -> participants
	// Accept() with a different argument
	RemoveMember(InstanceNumber, SequenceNumber, Member) error

	// Master -> new participant
	StartParticipation(i InstanceNumber, s SequenceNumber, cluster string, self Member, master Member, members []Member, snapshot []byte) error

	// Participant -> master (so that non-masters can submit changes)
	SubmitRequest([]Change) error
}

// Applications using this package need to implement this interface in order for the library to be able
// to send requests to other participants.
type ConsensusClient interface {
	ParticipantStub
	io.Closer
}

// This is implemented by Participant.
type ConsensusServer interface {
	ParticipantStub
}

// A component that receives external requests and calls a Participating component. Typically
// some RPC or HTTP server component that dispatches incoming requests to a participant.
type Server interface {
	Register(string, ConsensusServer) error
}
