# clusterconsensus

A library implementing a replicated state machine with generic state machine, transport and state changes.

For example, a using application could use this package with protocol buffers, leveldb and gRPC; and another
one might use JSON, in-memory state and HTTP.

## Protocol

The protocol is a custom Paxos implementation. It works like this, basically:

### Bootstrapping

* A set of participants is started. Participant `A`'s `ConsensusServer` receives a StartParticipation request.
  This request is `StartParticipation(0, 0, Member{addr: "A"}, [Member{addr: "A"}], []byte{})` (i.e., an empty
  StartParticipation request)
* Now one participant is running, and sees that it itself is the master. It now finds out in some way which other
  participants to add, and calls `AddMember()` on itself. The library will arrange a process by which a new member
  is added to the consensus round.
* Repeat this for other members

### Normal operation

We assume that the current instance is 12, and the sequence number in that instance is 34.

* One participant wants to modify the state. It calls `Submit()` on the `Participant` with one or more changes.
* If the Participant is not master, it will send the request to the master using the `Submit()` method on the stub.
* If the Participant is the master, or has received a `Submit` request, it will proceed by coordinating the change.
* First, all non-master participants are sent `Accept(12, 35, []Change{ *some change* })`.
* This leads to the non-master participants *staging* that change.
* The next time the master wants to apply a change, it sends `Accept(12, 36, []Change{ *some new change* })`. This leads
  to non-master participants "committing" (i.e. applying) all staged changes before sequence `36` to the state machine.

### Master has crashed

Current instance: 22 Current sequence: 44

* A participant, "B", wants to modify the state, but can't reach the master to `Submit()` a change. It proceeds by
  becoming a candidate. It sends `Prepare(23, Member{ addr: "B" })` to all known members. If a majority replies within
  the deadline with a positive vote, then the `B` is the new master.
* In order for all participants to know about this change, `B` submits a request `Accept(23, 1, []Change{})` which confirms
  the previous `Prepare()` call (because of the new instance in the `Accept` request)
* Now everyone knows of the new master, and the cluster continues in *Normal operation*.

### Adding/removing members

* Membership changes are relatively straight-forward; they are just special changes that don't use `Accept()`, but rather
  the `AddMember()` and `RemoveMember()` methods in `ParticipantStub`.

