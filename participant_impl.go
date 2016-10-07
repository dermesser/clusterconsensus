package clusterconsensus

import "fmt"

// This file contains methods on Participant to implement ParticipantStub. They are generally invoked
// by a clusterconsensus.Server, i.e. on request by a remote participant (including masters).

// From master
func (p *Participant) Prepare(i InstanceNumber, m Member) (InstanceNumber, error) {
	// 1. instance must be greater than current

	if i > p.instance {
		p.stagedChanges = make(map[SequenceNumber][]Change)
		p.stagedMembers = make(map[SequenceNumber]Member)
		// Stage current master. The master will be set once we receive an Accept() with this instance number.
		p.master[i] = m
		p.participantState = state_PENDING_MASTER
		return i, nil
	}

	return p.instance, nil
}

// From master
func (p *Participant) Accept(i InstanceNumber, s SequenceNumber, c []Change) (bool, error) {
	// 1. Do checks on supplied serial numbers
	if i < p.instance {
		return false, newError(ERR_DENIED, fmt.Sprintf("Instance %d less than current (%d)", i, p.instance), nil)
	}

	// s == p.sequence is allowed! (for non-committed Accept()s)
	if s < p.sequence {
		return false, newError(ERR_DENIED, fmt.Sprintf("Sequence %d less than current (%d)", s, p.sequence), nil)
	}

	// 2. If needed, unstage master by setting current instance

	if i >= p.instance {
		p.instance = i
	}

	// 3. If needed, commit previous changes

	for seq, changes := range p.stagedChanges {
		if seq < s {
			for _, c := range changes {
				p.state.Apply(c)
			}
		}
		delete(p.stagedChanges, seq)
	}

	for seq, member := range p.stagedMembers {
		if seq < s {
			p.members = append(p.members, member)

			if _, err := p.getConnectedClient(member); err == nil {
				delete(p.stagedMembers, seq)
			} // otherwise retry connecting on next accept
		}
	}

	// 4. Stage changes for commit

	// A zero-length Accept() is a pure commit
	if len(c) > 0 {
		p.stagedChanges[s] = c
		p.participantState = state_PARTICIPANT_PENDING
	} else {
		p.participantState = state_PARTICIPANT_CLEAN
	}

	return true, nil
}

// From master
func (p *Participant) AddMember(i InstanceNumber, s SequenceNumber, m Member) error {
	// 1. Do checks on supplied serial numbers
	if i < p.instance {
		return newError(ERR_DENIED, fmt.Sprintf("Instance %d less than current (%d)", i, p.instance), nil)
	}

	// s == p.sequence is allowed! (for non-committed Accept()s)
	if s < p.sequence {
		return newError(ERR_DENIED, fmt.Sprintf("Sequence %d less than current (%d)", s, p.sequence), nil)
	}

	// 2. Check that member is not already part of cluster here

	for _, existing := range p.members {
		if existing == m {
			return newError(ERR_DENIED, fmt.Sprintf("Member %v already exists here", m), nil)
		}
	}

	// 3. Stage member. Will be committed on next Accept() with higher sequence number

	p.stagedMembers[s] = m

	return nil
}

// From master
// If m is us, leave cluster
func (p *Participant) RemoveMember(i InstanceNumber, s SequenceNumber, m Member) error {
	// 1. Do checks on supplied serial numbers
	if i < p.instance {
		return newError(ERR_DENIED, fmt.Sprintf("Instance %d less than current (%d)", i, p.instance), nil)
	}

	// s == p.sequence is allowed! (for non-committed Accept()s)
	if s < p.sequence {
		return newError(ERR_DENIED, fmt.Sprintf("Sequence %d less than current (%d)", s, p.sequence), nil)
	}

	// 2. Check that member is not already part of cluster here

	for ix, existing := range p.members {
		if existing == m {
			// Remove member
			p.members = append(p.members[0:ix], p.members[ix+1:]...)

			if client, ok := p.participants[m]; ok {
				client.Close()
			}

			delete(p.participants, m)

			return nil
		}
	}

	// If it's us, leave cluster

	if p.self == m {
		// goodbye :(

		for _, client := range p.participants {
			client.Close()
		}

		p.members = nil
		p.master = nil
		p.participants = nil
		p.instance = (1 << 64) - 1 // make any elections impossible
		p.sequence = (1 << 64) - 1
		p.stagedChanges = nil
		p.stagedMembers = nil

		p.participantState = state_REMOVED
		return nil
	}

	return newError(ERR_DENIED, fmt.Sprintf("Member %v doesn't exist here", m), nil)
}

func (p *Participant) StartParticipation(i InstanceNumber, s SequenceNumber, self Member, master Member, members []Member, snapshot []byte) error {
	if p.participantState != state_UNJOINED {
		return newError(ERR_STATE, fmt.Sprintf("Expected state UNJOINED, am in state %d", p.participantState), nil)
	}

	p.instance = i
	p.sequence = s
	p.self = self
	p.members = members
	p.master[i] = master

	if len(members) == 1 && members[0] == master {
		// Bootstrapped externally
		p.participantState = state_MASTER
	} else {
		p.participantState = state_PARTICIPANT_CLEAN
	}

	p.state.Install(snapshot)

	for _, member := range members {
		// Try connecting already.
		p.getConnectedClient(member)
	}

	return nil
}

// RPC handler, not to be used locally. Only valid to be called on a master.
func (p *Participant) SubmitRequest(c []Change) error {
	// *Assert* that we're master

	if p.participantState != state_MASTER {
		return newError(ERR_STATE, "Can't request changes to be submitted on non-master", nil)
	} else {
		return p.submitAsMaster(c)
	}
}
