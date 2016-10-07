package clusterconsensus

import "fmt"

// This module implements local functionality for `Participant`, which is defined in types.rs.
// This means that the following (exported) functions are supposed to be called from the application that
// uses the clusterconsensus package.

func (p *Participant) GetState() State {
	return p.state
}

// Submit one change to the state machine
func (p *Participant) SubmitOne(c Change) error {
	return p.Submit([]Change{c})
}

// Submit submits a set of changes to the cluster. Returns nil if successful
// Depending on whether this Participant is currently a Master, this will either replicate the change to all
// non-master participants or send the change to the master which will then replicate the change.
func (p *Participant) Submit(c []Change) error {
	// 1. Check if we're master

	if p.participantState == state_MASTER {
		return p.submitAsMaster(c)
	} else if p.participantState == state_PARTICIPANT_CLEAN || p.participantState == state_PARTICIPANT_PENDING {
		return p.submitToRemoteMaster(c)
	} else if p.participantState == state_CANDIDATE || p.participantState == state_PENDING_MASTER {
		return newError(ERR_STATE, "Currently in candidate or unconfirmed-master state; try again later", nil)
	}

	return nil
}

func (p *Participant) submitAsMaster(c []Change) error {
	// Calculate majority. We ourselves count as accepting.
	// For 3 members, we need 1 other positive vote; for 5 members, we need 2 other votes
	requiredVotes := (len(p.members) - 1) / 2
	acquiredVotes := 0
	errs := []error{}

	// TODO: This can be made asynchronous

	// Send out Accept() requests
	for _, member := range p.members {
		if member == p.self {
			continue
		}

		client, err := p.getConnectedClient(member)

		if err != nil {
			return err
		}

		ok, err := client.Accept(p.instance, p.sequence+1, c)

		if err != nil {
			errs = append(errs, newError(ERR_CALL, "Error from remote participant", err))
			continue
		}
		if !ok {
			errs = append(errs, newError(ERR_DENIED, "Vote denied", nil))
			continue
		}

		acquiredVotes++
	}

	if acquiredVotes >= requiredVotes {
		// we got the majority
		p.sequence++
		// Now the next Accept() request will commit this submission to the state machine
	} else {
		return newError(ERR_MAJORITY, fmt.Sprintf("Missed majority: %d/%d. Errors: %v", acquiredVotes, requiredVotes, errs), nil)
	}

	return nil
}

// If submitting to the master fails, we will attempt to step up and become master ourselves, and then do the replication
// ourselves.
func (p *Participant) submitToRemoteMaster(c []Change) error {
	if p.participantState != state_PARTICIPANT_CLEAN && p.participantState != state_PARTICIPANT_PENDING {
		return newError(ERR_STATE, fmt.Sprintf("Expected PARTICIPANT_CLEAN or PARTICIPANT_PENDING, but is %d", p.participantState), nil)
	}

	master, ok := p.master[p.instance]

	if !ok {
		panic(fmt.Sprintf("Bad instance number - no master: %d/%v", p.instance, p.master))
	}

	masterConn, err := p.getConnectedClient(master)

	if err != nil {
		return err
	}

	// Send to remote master
	err = masterConn.SubmitRequest(c)

	if err == nil {
		return nil
	}

	err = p.tryBecomeMaster()

	if err != nil {
		return err // We tried everything
	} else {
		return p.submitAsMaster(c)
	}
}

func (p *Participant) tryBecomeMaster() error {
	// 1. Set state
	p.participantState = state_CANDIDATE

	// 2. Calculate votes
	requiredVotes := (len(p.members) - 1) / 2
	acquiredVotes := 0
	errs := []error{}

	for _, member := range p.members {
		if member == p.self {
			continue
		}
		client, err := p.getConnectedClient(member)

		if err != nil {
			errs = append(errs, newError(ERR_CONNECT, fmt.Sprintf("Error connecting to %v", member), err))
			continue
		}

		newInstance, err := client.Prepare(p.instance+1, p.self)

		if err != nil {
			errs = append(errs, newError(ERR_CALL, fmt.Sprintf("Error calling Prepare() on %v", member), err))
			continue
		}
		if newInstance != p.instance+1 {
			errs = append(errs, newError(ERR_DENIED, fmt.Sprintf("Vote denied; proposal %d, response %d", p.instance+1, newInstance), nil))
			continue
		}

		acquiredVotes++
	}

	if acquiredVotes < requiredVotes {
		p.participantState = state_PENDING_MASTER
		return newError(ERR_MAJORITY, fmt.Sprintf("No majority in master election: %v", errs), nil)
	}

	p.participantState = state_MASTER
	return nil
}

// Look up connection for member, and connect if necessary.
func (p *Participant) getConnectedClient(m Member) (ConsensusClient, error) {
	client, ok := p.participants[m]

	// Try connect
	if !ok {
		var err error
		client, err = p.connFactory.Connect(p.cluster, m)

		if err != nil {
			return nil, newError(ERR_CONNECT, fmt.Sprintf("Could not connect to %v", m), err)
		} else {
			p.participants[m] = client
			return client, nil
		}
	}

	return client, nil
}

// Local method:
// Add a member to the cluster that we're the master of. Fails if not master.
func (p *Participant) AddParticipant(m Member) error {
	if p.participantState != state_MASTER {
		return newError(ERR_STATE, "Expected to be MASTER", nil)
	}

	// 1. Check if already in cluster
	for _, existing := range p.members {
		if existing == m {
			return newError(ERR_STATE, "Participant already exists in cluster", nil)
		}
	}

	// 2. Ask other participants to add new member

	requiredVotes := (len(p.members) - 1) / 2
	acquiredVotes := 0
	errs := []error{}

	for _, member := range p.members {
		if member == p.self {
			continue
		}
		client, err := p.getConnectedClient(member)

		if err != nil {
			errs = append(errs, newError(ERR_CONNECT, fmt.Sprintf("Error connecting to %v", member), err))
			continue
		}

		err = client.AddMember(p.instance, p.sequence+1, m)

		if err != nil {
			errs = append(errs, newError(ERR_CALL, fmt.Sprintf("Error calling Prepare() on %v", member), err))
			continue
		}

		acquiredVotes++
	}

	if acquiredVotes < requiredVotes {
		return newError(ERR_MAJORITY, fmt.Sprintf("No majority in master election: %v", errs), nil)
	}

	p.sequence++

	p.members = append(p.members, m)
	client, err := p.getConnectedClient(m)

	if err != nil {
		return newError(ERR_CALL, fmt.Sprintf("Couldn't call StartParticipation() on %v", m), err)
	}

	return client.StartParticipation(p.instance, p.sequence, m, p.self, p.members, p.state.Snapshot())
}

func (p *Participant) RemoveParticipant(m Member) error {
	if p.participantState != state_MASTER {
		return newError(ERR_STATE, "Expected to be MASTER", nil)
	}

	for ix, existing := range p.members {
		if existing == m {
			requiredVotes := (len(p.members) - 1) / 2
			acquiredVotes := 0
			errs := []error{}

			for _, member := range p.members {
				if member == p.self {
					continue
				}

				client, err := p.getConnectedClient(member)

				if err != nil {
					errs = append(errs, newError(ERR_CONNECT, fmt.Sprintf("Error connecting to %v", member), err))
					continue
				}

				err = client.RemoveMember(p.instance, p.sequence+1, m)

				if err != nil {
					errs = append(errs, newError(ERR_CALL, fmt.Sprintf("Error calling RemoveMember() on %v", member), nil))
					continue
				}

				acquiredVotes++
			}

			if acquiredVotes < requiredVotes {
				return newError(ERR_MAJORITY, fmt.Sprintf("No majority for RemoveMember(); errors: %v", errs), nil)
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

	return newError(ERR_STATE, "Participant doesn't exist in cluster", nil)

}
