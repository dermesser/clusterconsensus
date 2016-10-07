package clusterconsensus

import "fmt"

// This file contains methods on Participant to implement ParticipantStub. They are generally invoked
// by a clusterconsensus.Server, i.e. on request by a remote participant (including masters).

func (p *Participant) getMaster() (Member, bool) {
	m, ok := p.master[p.instance]
	return m, ok
}

// Handler:
// From master
// Asks for a vote for m to become master of instance i.
func (p *Participant) Prepare(i InstanceNumber, m Member) (InstanceNumber, error) {
	if p.participantState == state_REMOVED || p.participantState == state_UNJOINED {
		return 0, newError(ERR_STATE, "Prepare() called on unjoined or removed participant", nil)
	}

	// 1. instance must be greater than current

	if i > p.instance {
		// Stage current master. The master will be set once we receive an Accept() with this instance number.
		p.master[i] = m
		p.participantState = state_PENDING_MASTER
		return i, nil
	}

	return p.instance, nil
}

// Handler:
// From master
// Asks to accept the changes c for round s of instance i.
func (p *Participant) Accept(i InstanceNumber, s SequenceNumber, c []Change) (bool, error) {
	if p.participantState == state_REMOVED || p.participantState == state_UNJOINED {
		return false, newError(ERR_STATE, "Accept() called on unjoined participant", nil)
	}

	// 1. Do checks on supplied serial numbers
	if i < p.instance {
		return false, newError(ERR_DENIED, fmt.Sprintf("Instance %d less than current (%d)", i, p.instance), nil)
	}

	// s == p.sequence is allowed! (for non-committed Accept()s)
	if s < p.sequence {
		return false, newError(ERR_DENIED, fmt.Sprintf("Sequence %d less than current (%d)", s, p.sequence), nil)
	}

	// 2., 3.
	p.commitStagedChanges(i, s)

	if p.participantState == state_REMOVED {
		return true, nil
	}

	// 4. Stage changes for commit

	// A zero-length Accept() is a pure commit
	if len(c) > 0 {
		p.stagedChanges[s] = c
		delete(p.stagedMembers, s)
		delete(p.stagedRemovals, s)
		p.participantState = state_PARTICIPANT_PENDING
	} else {
		p.participantState = state_PARTICIPANT_CLEAN
	}

	return true, nil
}

// Commit all changes up to and including s-1, and sets p.sequence = s
// If we're in a new instance, clean up all staged changes.
// Otherwise: Commit changes, add members, remove members and leave the cluster if needed (i.e. committing a prior removal)
func (p *Participant) commitStagedChanges(i InstanceNumber, s SequenceNumber) {
	// 1. If needed, unstage master by setting current instance.
	// Reset everything in case we missed the election.

	if i > p.instance {
		p.stagedChanges = make(map[SequenceNumber][]Change)
		p.stagedMembers = make(map[SequenceNumber]Member)
		p.stagedRemovals = make(map[SequenceNumber]Member)
		p.participantState = state_PARTICIPANT_CLEAN

		p.instance = i

		return
	}

	// 2. If needed, commit previous changes

	for seq := p.sequence; seq < s; s++ {
		if seq < s {
			if changes, ok := p.stagedChanges[seq]; ok {
				for _, c := range changes {
					p.state.Apply(c)
				}
			}
		}
		delete(p.stagedChanges, seq)
	}

	// 3. and add staged member

	for seq := p.sequence; seq < s; s++ {
		if seq < s {
			if member, ok := p.stagedMembers[seq]; ok {
				p.members = append(p.members, member)

				if _, err := p.getConnectedClient(member); err == nil {
					delete(p.stagedMembers, seq)
				} // otherwise retry connecting on next accept
			}
		}
	}

	// 4. and commit staged removals

outer:
	for seq := p.sequence; seq < s; s++ {
		if m, ok := p.stagedRemovals[seq]; ok {
			for ix, existing := range p.members {
				if existing == m {
					if p.self == m {
						p.removeFromCluster()
					} else {
						// Remove member
						p.members = append(p.members[0:ix], p.members[ix+1:]...)

						if client, ok := p.participants[m]; ok {
							client.Close()
						}

						p.participantState = state_PARTICIPANT_PENDING
						delete(p.participants, m)
					}
					break outer
				}
			}
		}
	}

	p.sequence = s
}

// Handler:
// From master
// Asks to add member m to cluster in instance i, round s.
func (p *Participant) AddMember(i InstanceNumber, s SequenceNumber, m Member) error {
	if p.participantState == state_REMOVED || p.participantState == state_UNJOINED {
		return newError(ERR_STATE, "AddMember() called on removed or unjoined participant", nil)
	}

	// 1. Do checks on supplied serial numbers
	if i < p.instance {
		return newError(ERR_DENIED, fmt.Sprintf("Instance %d less than current (%d)", i, p.instance), nil)
	}

	// s == p.sequence is allowed! (for non-committed Accept()s)
	if s < p.sequence {
		return newError(ERR_DENIED, fmt.Sprintf("Sequence %d less than current (%d)", s, p.sequence), nil)
	}

	p.commitStagedChanges(i, s)

	if p.participantState == state_REMOVED {
		return nil
	}

	// 2. Check that member is not already part of cluster here

	for _, existing := range p.members {
		if existing == m {
			return newError(ERR_DENIED, fmt.Sprintf("Member %v already exists here", m), nil)
		}
	}

	// 3. Stage member. Will be committed on next Accept() with higher sequence number

	p.stagedMembers[s] = m
	delete(p.stagedChanges, s)
	delete(p.stagedRemovals, s)

	p.participantState = state_PARTICIPANT_PENDING
	return nil
}

// Handler:
// From master
// Asks to remove member m in instance i, round s from the cluster. Removes p from cluster
// if m describes p.
func (p *Participant) RemoveMember(i InstanceNumber, s SequenceNumber, m Member) error {
	if p.participantState == state_REMOVED || p.participantState == state_UNJOINED {
		return newError(ERR_STATE, "RemoveMember() called on removed or unjoined participant", nil)
	}

	// 1. Do checks on supplied serial numbers
	if i < p.instance {
		return newError(ERR_DENIED, fmt.Sprintf("Instance %d less than current (%d)", i, p.instance), nil)
	}

	// s == p.sequence is allowed! (for non-committed Accept()s)
	if s < p.sequence {
		return newError(ERR_DENIED, fmt.Sprintf("Sequence %d less than current (%d)", s, p.sequence), nil)
	}

	p.commitStagedChanges(i, s)

	if p.participantState == state_REMOVED {
		return nil
	}

	// 2. Check that member is not already part of cluster here

	for _, existing := range p.members {
		if existing == m {
			// The removal will only happen on commit.
			p.stagedRemovals[s] = m
			delete(p.stagedChanges, s)
			delete(p.stagedMembers, s)
			return nil
		}
	}

	return newError(ERR_DENIED, fmt.Sprintf("Member %v doesn't exist here", m), nil)
}

func (p *Participant) removeFromCluster() {
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
}

// From master:
// Handler
// Asks p to start participating in a cluster.
func (p *Participant) StartParticipation(i InstanceNumber, s SequenceNumber, cluster string, self Member, master Member, members []Member, snapshot []byte) error {
	if p.participantState != state_UNJOINED {
		return newError(ERR_STATE, fmt.Sprintf("Expected state UNJOINED, am in state %d", p.participantState), nil)
	}

	p.cluster = cluster
	p.members = members
	p.master[i] = master
	p.self = self
	p.instance = i
	p.sequence = s
	p.state.Install(snapshot)

	if len(members) == 1 && members[0] == master {
		// Bootstrapped externally
		p.participantState = state_MASTER
	} else {
		p.participantState = state_PARTICIPANT_CLEAN
	}

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
