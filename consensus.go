package clusterconsensus

import "fmt"

// Public API of Participant (without the ParticipantStub methods).
// These methods are typically called locally by an application using this library.

// Set up a new participant. Proceed by Register()ing it with a clusterconsensus.Server, and
// calling InitMaster() if this participant is the initial master (otherwise send a StartParticipation
// request to the server, as described in README.md)
func NewParticipant(cluster string, connector Connector, initialState State) *Participant {
	return &Participant{
		cluster: cluster,
		members: []Member{},
		master:  make(map[InstanceNumber]Member),
		self:    Member{},

		participants: make(map[Member]ConsensusClient),

		instance: 0,
		sequence: 0,

		state:            initialState,
		participantState: state_UNJOINED,

		stagedChanges:  make(map[SequenceNumber][]Change),
		stagedMembers:  make(map[SequenceNumber]Member),
		stagedRemovals: make(map[SequenceNumber]Member),

		connector: connector,
	}
}

// If a participant is supposed to be the first member and master, call InitMaster(). After that, you
// can call AddParticipant() to add more members to the cluster.
func (p *Participant) InitMaster(self Member, snapshot []byte) {
	p.StartParticipation(1, 0, p.cluster, self, self, []Member{self}, snapshot)
}

// This module implements local functionality for `Participant`, which is defined in types.rs.
// This means that the following (exported) functions are supposed to be called from the application that
// uses the clusterconsensus package.

// Return the current state. Note: This is only the strongly consistent current state if IsMaster() is true.
func (p *Participant) GetState() State {
	return p.state
}

// Returns whether this participant is the current master.
func (p *Participant) IsMaster() bool {
	if master, ok := p.master[p.instance]; ok {
		return p.self == master
	} else {
		return false
	}
}

// Returns true if there is an elected master.
func (p *Participant) HasMaster() bool {
	return p.CurrentMaster() != ""
}

// Returns the address of the current master, or an empty string if there is no master.
func (p *Participant) CurrentMaster() string {
	if master, ok := p.master[p.instance]; ok {
		return master.Address
	} else {
		return ""
	}
}

// Initiate an election and try to become master.
// This will not work if this participant's state is not as up to date as all other participants.
func (p *Participant) StartElection() error {
	if err := p.tryBecomeMaster(); err != nil {
		return err
	} else {
		return p.Submit([]Change{})
	}
}

// Sends a no-op to the master and returns nil if the master is up and active.
func (p *Participant) PingMaster() error {
	return p.Submit([]Change{})
}

// Set the handler for events.
func (p *Participant) SetEventHandler(eh EventHandler) {
	p.eventHandler = eh
}

// Submit one change to the state machine
func (p *Participant) SubmitOne(c Change) error {
	if p.IsMaster() {
		return nil
	}
	return p.Submit([]Change{c})
}

// Submit submits a set of changes to the cluster. Returns nil if successful
// Depending on whether this Participant is currently a Master, this will either replicate the change to all
// non-master participants or send the change to the master which will then replicate the change.
//
// NOTE -- after this returns, the changes are only fully committed on the master. To achieve full
// commit across all participants, just submit another (set of) change(s), which may be empty.
// In normal operation, if you submit a stream of changes, any change will be committed durably
// across the cluster once the consecutive change has been submitted (this design decision has been
// made for latency/performance reasons)
func (p *Participant) Submit(c []Change) error {
	// 1. Check if we're master

	if p.participantState == state_MASTER {
		return p.submitAsMaster(c)
	} else if p.participantState == state_PARTICIPANT_CLEAN || p.participantState == state_PARTICIPANT_PENDING {
		err := p.submitToRemoteMaster(c)
		if err != nil {
			err = p.tryBecomeMaster()

			if err != nil {
				return err
			}

			return p.submitAsMaster(c)
		}
	} else if p.participantState == state_CANDIDATE || p.participantState == state_PENDING_MASTER || p.participantState == state_UNJOINED {
		return NewError(ERR_STATE, "Currently in candidate or unconfirmed-master state; try again later", nil)
	}

	return nil
}

// Local method:
// Add a member to the cluster that we're the master of. Fails if not master.
func (p *Participant) AddParticipant(m Member) error {
	p.Lock()
	defer p.Unlock()

	if p.participantState != state_MASTER {
		return NewError(ERR_STATE, "Expected to be MASTER", nil)
	}

	// 1. Check if already in cluster
	for _, existing := range p.members {
		if existing == m {
			return NewError(ERR_STATE, "Participant already exists in cluster", nil)
		}
	}

	// 2. Ask other participants to add new member

	requiredVotes := (len(p.members) - 1) / 2
	acquiredVotes := 0
	errs := []error{}

	// TODO: This can be made asynchronous/concurrently
	// TODO: Use contexts
	for _, member := range p.members {
		if member == p.self {
			continue
		}
		client, err := p.getConnectedClient(member)

		if err != nil {
			errs = append(errs, NewError(ERR_CONNECT, fmt.Sprintf("Error connecting to %v", member), err))
			continue
		}

		err = client.AddMember(p.instance, p.sequence+1, m)

		if err != nil {
			errs = append(errs, NewError(ERR_CALL, fmt.Sprintf("Error calling Prepare() on %v", member), err))
			continue
		}

		acquiredVotes++
	}

	if acquiredVotes < requiredVotes {
		return NewError(ERR_MAJORITY, fmt.Sprintf("No majority in master election: %v", errs), nil)
	}

	p.sequence++

	p.members = append(p.members, m)
	client, err := p.getConnectedClient(m)

	if err != nil {
		return NewError(ERR_CALL, fmt.Sprintf("Couldn't call StartParticipation() on %v", m), err)
	}

	return client.StartParticipation(p.instance, p.sequence, p.cluster, m, p.self, p.members, p.state.Snapshot())
}

func (p *Participant) RemoveParticipant(m Member) error {
	p.Lock()
	defer p.Unlock()

	if p.participantState != state_MASTER {
		return NewError(ERR_STATE, "Expected to be MASTER", nil)
	}

	for ix, existing := range p.members {
		if existing == m {
			requiredVotes := (len(p.members) - 1) / 2
			acquiredVotes := 0
			errs := []error{}

			// TODO: This can be made asynchronous/concurrently
			// TODO: Use contexts
			for _, member := range p.members {
				if member == p.self {
					continue
				}

				client, err := p.getConnectedClient(member)

				if err != nil {
					errs = append(errs, NewError(ERR_CONNECT, fmt.Sprintf("Error connecting to %v", member), err))
					continue
				}

				err = client.RemoveMember(p.instance, p.sequence+1, m)

				if err != nil {
					errs = append(errs, NewError(ERR_CALL, fmt.Sprintf("Error calling RemoveMember() on %v", member), nil))
					continue
				}

				acquiredVotes++
			}

			if acquiredVotes < requiredVotes {
				return NewError(ERR_MAJORITY, fmt.Sprintf("No majority for RemoveMember(); errors: %v", errs), nil)
			}

			// commit. The next accept() with the new sequence number will remove it on all clients.
			p.sequence++
			p.members = append(p.members[0:ix], p.members[ix+1:]...)

			if client, ok := p.participants[m]; ok {
				client.Close()
				delete(p.participants, m)
			}

			break
		}
	}

	return NewError(ERR_STATE, "Participant doesn't exist in cluster", nil)

}
