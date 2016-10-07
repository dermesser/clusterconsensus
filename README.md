# clusterconsensus

A library implementing a replicated state machine with generic state machine, transport and state changes.

For example, a using application could use this package with protocol buffers, leveldb and gRPC; and another
one might use JSON, in-memory state and HTTP.

## Protocol

The protocol is a custom Paxos implementation. It works like this, basically:

### Bootstrapping

* A set of participants is started. 
* On the participant that has been selected to become the first master, `InitMaster()` is called.
* After `InitMaster()`, `AddParticipants()` adds more participants to the cluster. The list of participants
  could for example come from a configuration file or command line flags.
* Then, the cluster should be up and running. Now, on any participant, `Submit()` can be called to submit
  changes to the state machine. Those changes are replicated through the master. (see next section)

### Normal operation

We assume that the current instance is 12, and the sequence number in that instance is 34.

* One participant wants to modify the state. It calls `Submit()` on the `Participant` with one or more changes.
    * If the Participant is not master, it will send the request to the master using the `Submit()` method on the stub.
    * If the Participant is the master, or has received a `Submit` request, it will proceed by coordinating the change.
* First, all non-master participants are sent `Accept(12, 35, []Change{ *some change* })`.
    * The request is sent using the `ConsensusClient` stub that was returned by the `ClientFactory` implementation.
* This leads to the non-master participants *staging* that change into a special area. The change is not yet applied to the state machine.
* The next time the master wants to apply another change, it sends `Accept(12, 36, []Change{ *some new change* })`. This leads
  to non-master participants "committing" (i.e. applying) all staged changes before sequence `36` to the state machine, including
  our change `35` from before.

### Master has crashed

Current instance: 22; Current sequence: 44

* A participant, "B", wants to modify the state, but can't reach the master to `Submit()` a change. It proceeds by
  becoming a candidate. It sends `Prepare(23, Member{ addr: "B" })` to all known members. If a majority replies within
  the deadline with a positive vote, then `B` is the new master.
* In order for all participants to know about this change, `B` submits a request `Accept(23, 1, []Change{})` which confirms
  the previous `Prepare()` call, and makes all non-master participants acknowledge the new master.
* Now everyone knows of the new master, and the cluster continues as in *Normal operation*.

### Adding/removing members

* Membership changes are relatively straight-forward; they are just special changes that don't use `Accept()`, but rather
  the `AddMember()` and `RemoveMember()` methods in `ParticipantStub`.
* Membership changes are staged the same way as normal changes; memberships only actually change once a membership change
  has been committed by an `Accept()` call with a higher sequence number.
* Membership adds are communicated to all old members. After successfully staging the change (i.e., getting a majority vote),
  the new member is started using `StartParticipation()`.
* Membership removals are communicated to all members, including the one to be removed. Upon 
