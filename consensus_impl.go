package clusterconsensus

import "fmt"

// methods that are only used by public methods on Participant.

func (p *Participant) submitAsMaster(c []Change) error {
	p.Lock()
	defer p.Unlock()

	// Calculate majority. We ourselves count as accepting.
	// For 3 members, we need 1 other positive vote; for 5 members, we need 2 other votes
	requiredVotes := (len(p.members) - 1) / 2
	acquiredVotes := 0
	errs := []error{}

	// TODO: This can be made asynchronous/concurrently
	// TODO: Use contexts

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
			errs = append(errs, NewError(ERR_CALL, "Error from remote participant", err))

			// force: re-send snapshot if the client has seen a gap

			// Useful to solve generic network errors
			p.forceReconnect(member)

			// Especially useful to solve ERR_STATE, ERR_GAP errors
			client.StartParticipation(p.instance, p.sequence, p.cluster, member, p.self, p.members, p.state.Snapshot())

			ok, err := client.Accept(p.instance, p.sequence+1, c)

			if ok && err == nil {
				acquiredVotes++
			}

			continue
		}
		if !ok {
			errs = append(errs, NewError(ERR_DENIED, "Vote denied", nil))
			continue
		}

		acquiredVotes++
	}

	if acquiredVotes >= requiredVotes {
		// we got the majority
		p.sequence++

		// Now the next Accept() request will commit this submission to the remote state machines

		for _, chg := range c {
			p.state.Apply(chg)
		}
	} else {
		return NewError(ERR_MAJORITY, fmt.Sprintf("Missed majority: %d/%d. Errors: %v", acquiredVotes, requiredVotes, errs), nil)
	}

	return nil
}

// If submitting to the master fails, we will attempt to step up and become master ourselves, and then do the replication
// ourselves.
func (p *Participant) submitToRemoteMaster(c []Change) error {
	if p.participantState != state_PARTICIPANT_CLEAN && p.participantState != state_PARTICIPANT_PENDING {
		return NewError(ERR_STATE, fmt.Sprintf("Expected PARTICIPANT_CLEAN or PARTICIPANT_PENDING, but is %d", p.participantState), nil)
	}

	master, ok := p.getMaster()

	if !ok {
		panic(fmt.Sprintf("Bad instance number - no master: %d/%v", p.instance, p.master))
	}

	masterConn, err := p.getConnectedClient(master)

	if err != nil {
		return err
	}

	// Send to remote master
	err = masterConn.SubmitRequest(c)

	return err
}

func (p *Participant) tryBecomeMaster() error {
	p.Lock()
	defer p.Unlock()

	// 1. Set state
	p.participantState = state_CANDIDATE

	// 2. Calculate votes
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

		newInstance, err := client.Prepare(p.instance+1, p.self)

		if err != nil {
			errs = append(errs, NewError(ERR_CALL, fmt.Sprintf("Error calling Prepare() on %v", member), err))
			continue
		}
		if newInstance > p.instance+1 {
			return NewError(ERR_DENIED, fmt.Sprintf("We don't have an up-to-date local state (instance %d/%d)", p.instance+1, newInstance), nil)
		}

		acquiredVotes++
	}

	if acquiredVotes < requiredVotes {
		p.participantState = state_PARTICIPANT_CLEAN
		return NewError(ERR_MAJORITY, fmt.Sprintf("No majority in master election: %v", errs), nil)
	}

	p.instance++
	p.master[p.instance] = p.self
	p.sequence = 0
	p.participantState = state_MASTER
	return nil
}

// Look up connection for member, and connect if necessary.
func (p *Participant) getConnectedClient(m Member) (ConsensusClient, error) {
	client, ok := p.participants[m]

	// Try connect
	if !ok {
		var err error
		client, err = p.connector.Connect(p.cluster, m)

		if err != nil {
			return nil, NewError(ERR_CONNECT, fmt.Sprintf("Could not connect to %v", m), err)
		} else {
			p.participants[m] = client
			return client, nil
		}
	}

	return client, nil
}

func (p *Participant) forceReconnect(m Member) (ConsensusClient, error) {
	client, ok := p.participants[m]

	if ok {
		client.Close()
		delete(p.participants, m)
	}

	return p.getConnectedClient(m)
}
