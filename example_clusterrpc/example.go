package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"strconv"
	"strings"
	"time"

	con "bitbucket.org/dermesser/clusterconsensus"
	proto "bitbucket.org/dermesser/clusterconsensus/example_clusterrpc/proto"
	_ "bitbucket.org/dermesser/clusterrpc"
	rpccl "bitbucket.org/dermesser/clusterrpc/client"
	rpcsrv "bitbucket.org/dermesser/clusterrpc/server"
	"github.com/golang/glog"
	pb "github.com/golang/protobuf/proto"
)

const (
	change_ADD string = "ADD"
	change_RM         = "RM"

	SERVICE = "Consensus"
)

var (
	CLIENT_ID string = "_default"
)

type Connector struct {
}

type client struct {
	cl      *rpccl.Client
	host    string
	cluster string
}

func (c *Connector) Connect(cluster string, m con.Member) (con.ConsensusClient, error) {
	addr := m.Address
	host := strings.Split(addr, ":")[0]
	port := strings.Split(addr, ":")[1]
	iport, err := strconv.ParseUint(port, 10, 32)
	if err != nil {
		return nil, err
	}
	glog.Info("connecting to ", host, ":", iport)
	ch, err := rpccl.NewChannelAndConnect(rpccl.Peer(host, uint(iport)), nil)
	if err != nil {
		glog.Error("connect failed: ", err)
		return nil, err
	}
	cl := rpccl.New(CLIENT_ID, ch)
	cl.SetTimeout(1500*time.Millisecond, true)
	return &client{cl: &cl, host: addr, cluster: cluster}, nil
}

func (c *client) Close() error {
	return nil
}

func (c *client) Prepare(i con.InstanceNumber, m con.Member) (con.InstanceNumber, error) {
	glog.Info("Prepare sent to ", c.host)
	req := proto.PrepareRequest{Instance: pb.Uint64(uint64(i)),
		Master: &proto.Member{Address: pb.String(m.Address)}, Cluster: pb.String(c.cluster)}
	rpcReq := c.cl.NewRequest(SERVICE, "Prepare")
	if rpcReq == nil {
		return 0, errors.New("request is in progress")
	}
	resp := rpcReq.GoProto(&req)
	if !resp.Ok() {
		glog.Error(c.host, ": RPC error in Prepare: ", resp.Error())
		return 0, errors.New(resp.Error())
	}
	var respMsg proto.PrepareResponse
	if err := resp.GetResponseMessage(&respMsg); err != nil {
		return 0, err
	}
	return con.InstanceNumber(respMsg.GetInstance()), nil
}

func (c *client) Accept(i con.InstanceNumber, s con.SequenceNumber, chgs []con.Change) (bool, error) {
	glog.Info("Accept ", i, s, " sent to ", c.host)
	defer glog.Info("Accept ", i, s, " to ", c.host, " finished")
	version := &proto.Version{Instance: pb.Uint64(uint64(i)), Sequence: pb.Uint64(uint64(s))}
	changes := make([]*proto.Change, len(chgs))
	for i := range chgs {
		changes[i] = &proto.Change{Change: chgs[i].Serialize()}
	}
	req := proto.AcceptRequest{Version: version, Changes: changes, Cluster: pb.String(c.cluster)}

	rpcReq := c.cl.NewRequest(SERVICE, "Accept")
	if rpcReq == nil {
		return false, errors.New("request is in progress")
	}
	resp := rpcReq.GoProto(&req)
	if !resp.Ok() {
		glog.Error(c.host, ": RPC error in Accept: ", resp.Error())
		return false, errors.New(resp.Error())
	}
	var respMsg proto.GenericResponse
	if err := resp.GetResponseMessage(&respMsg); err != nil {
		return false, err
	}
	if respMsg.GetError() != nil {
		glog.Error(c.host, ": Consensus error: ", resp.Error())
		return false, errors.New(respMsg.GetError().GetError())
	}
	return true, nil
}

func (c *client) AddMember(i con.InstanceNumber, s con.SequenceNumber, m con.Member) error {
	version := &proto.Version{Instance: pb.Uint64(uint64(i)), Sequence: pb.Uint64(uint64(s))}
	req := &proto.AddMemberRequest{Version: version, Member: &proto.Member{Address: &m.Address}, Cluster: &c.cluster}
	rpcReq := c.cl.NewRequest(SERVICE, "AddMember")
	if rpcReq == nil {
		return errors.New("request is in progress")
	}
	resp := rpcReq.GoProto(req)
	if !resp.Ok() {
		glog.Error(c.host, ": RPC error in AddMember: ", resp.Error())
		return errors.New(resp.Error())
	}
	var respMsg proto.GenericResponse
	if err := resp.GetResponseMessage(&respMsg); err != nil {
		return err
	}
	if !respMsg.GetAccepted() {
		return errors.New("AddMember not accepted")
	}
	return nil
}

func (c *client) RemoveMember(i con.InstanceNumber, s con.SequenceNumber, m con.Member) error {
	version := &proto.Version{Instance: pb.Uint64(uint64(i)), Sequence: pb.Uint64(uint64(s))}
	req := &proto.RemoveMemberRequest{Version: version, Member: &proto.Member{Address: &m.Address}, Cluster: &c.cluster}
	rpcReq := c.cl.NewRequest(SERVICE, "RemoveMember")
	if rpcReq == nil {
		return errors.New("request is in progress")
	}
	resp := rpcReq.GoProto(req)
	if !resp.Ok() {
		glog.Error(c.host, ": RPC error in RemoveMember: ", resp.Error())
		return errors.New(resp.Error())
	}
	var respMsg proto.GenericResponse
	if err := resp.GetResponseMessage(&respMsg); err != nil {
		return err
	}
	if !respMsg.GetAccepted() {
		return errors.New("RemoveMember not accepted")
	}
	return nil
}

func (c *client) StartParticipation(i con.InstanceNumber,
	s con.SequenceNumber,
	cluster string,
	self con.Member,
	master con.Member,
	members []con.Member,
	snapshot []byte) error {
	glog.Info("StartParticipation sent to ", self.Address)

	participants := make([]*proto.Member, len(members))
	for i := range members {
		participants[i] = &proto.Member{Address: &members[i].Address}
	}
	req := &proto.StartParticipation{
		Version:  &proto.Version{Instance: pb.Uint64(uint64(i)), Sequence: pb.Uint64(uint64(s))},
		Cluster:  &c.cluster,
		Self:     &proto.Member{Address: &self.Address},
		Master:   &proto.Member{Address: &master.Address},
		Members:  participants,
		Snapshot: snapshot}
	var respMsg proto.GenericResponse
	rpcReq := c.cl.NewRequest(SERVICE, "StartParticipation")
	if rpcReq == nil {
		return errors.New("request is in progress")
	}
	resp := rpcReq.GoProto(req)
	if !resp.Ok() {
		glog.Error(c.host, ": RPC error in StartParticipation: ", resp.Error())
		return errors.New(resp.Error())
	}
	resp.GetResponseMessage(&respMsg)
	if !respMsg.GetAccepted() {
		return errors.New("StartParticipation not accepted")
	}
	return nil
}

func (c *client) SubmitRequest(chg []con.Change) error {
	glog.Info("Submitting ", len(chg), " changes to ", c.host)

	changes := make([]*proto.Change, len(chg))
	for i := range chg {
		changes[i] = &proto.Change{Change: chg[i].Serialize()}
	}
	req := &proto.SubmitRequest{Cluster: &c.cluster, Changes: changes}
	var respMsg proto.GenericResponse
	rpcReq := c.cl.NewRequest(SERVICE, "Submit")
	if rpcReq == nil {
		return errors.New("request is in progress")
	}
	resp := rpcReq.GoProto(req)
	if !resp.Ok() {
		glog.Error(c.host, ": RPC error in Submit: ", resp.Error())
		return errors.New(resp.Error())
	}
	resp.GetResponseMessage(&respMsg)
	if !respMsg.GetAccepted() {
		return errors.New("Submit not accepted!")
	}
	return nil
}

// Simple state machine
type State struct {
	inner map[string]string
}

func (s State) Snapshot() []byte {
	buf := bytes.NewBuffer([]byte{})

	for k, v := range s.inner {
		buf.WriteString(k)
		buf.WriteString("×")
		buf.WriteString(v)
		buf.WriteString("×")
	}

	return buf.Bytes()
}

func (s State) Apply(c con.Change) {
	chg := c.(Change)

	glog.Info("Applying", chg)

	if chg.t == change_ADD {
		s.inner[chg.key] = chg.val
	} else if chg.t == change_RM {
		delete(s.inner, chg.key)
	}
}

func (s State) Install(ss []byte) {
	parts := strings.Split(string(ss), "×")

	for i := 0; i < len(parts)-1; {
		key := parts[i]
		i++
		val := parts[i]
		i++
		s.inner[key] = val
	}
}

// Change to state machine
type Change struct {
	t   string
	key string
	val string
}

func (c Change) Serialize() []byte {
	return []byte(fmt.Sprintf("%s×%s×%s", c.t, c.key, c.val))
}

type ChangeDeserializer struct{}

func (cd ChangeDeserializer) Deserialize(b []byte) con.Change {
	parts := strings.Split(string(b), "×")

	return Change{t: parts[0], key: parts[1], val: parts[2]}
}

type EventHandler struct{}

func (eh EventHandler) OnBecomeMaster(*con.Participant) {
	glog.Info("BECAME MASTER")
}

func (eh EventHandler) OnLoseMaster(*con.Participant) {
	glog.Info("LOST MASTERSHIP")
}

func (eh EventHandler) OnCommit(p *con.Participant, s con.SequenceNumber, chg []con.Change) {
	glog.Info("COMMITTED: ", s, chg)
}

type RpcServer struct {
	// cluster/server mapping
	participants map[string]con.ConsensusServer
	server       *rpcsrv.Server
}

func NewRpcServer(host string, port uint) (*RpcServer, error) {
	srv, err := rpcsrv.NewServer(host, port, 1, nil)
	if err != nil {
		return nil, err
	}
	srv.SetMachineName(fmt.Sprintf("port:%d", port))

	rpcServer := &RpcServer{participants: map[string]con.ConsensusServer{}, server: srv}
	srv.RegisterHandler(SERVICE, "Prepare", rpcServer.handlePrepare)
	srv.RegisterHandler(SERVICE, "Accept", rpcServer.handleAccept)
	srv.RegisterHandler(SERVICE, "AddMember", rpcServer.handleAddMember)
	srv.RegisterHandler(SERVICE, "RemoveMember", rpcServer.handleRmMember)
	srv.RegisterHandler(SERVICE, "StartParticipation", rpcServer.handleStart)
	srv.RegisterHandler(SERVICE, "Submit", rpcServer.handleSubmit)
	go srv.Start()
	return rpcServer, nil
}

func (srv *RpcServer) handlePrepare(ctx *rpcsrv.Context) {
	var req proto.PrepareRequest
	var resp proto.PrepareResponse

	if err := ctx.GetArgument(&req); err != nil {
		glog.Error("Couldn't parse prepare request:", err)
		ctx.Fail("couldn't parse request")
		return
	}
	inner, ok := srv.participants[req.GetCluster()]
	if !ok {
		glog.Error("bad cluster", req.GetCluster())
		ctx.Fail("bad cluster")
		return
	}

	glog.Info("server: prepare:", req.String(), " by ", ctx.GetClientId())
	inst, err := inner.Prepare(con.InstanceNumber(req.GetInstance()), con.Member{Address: req.GetMaster().GetAddress()})
	if err != nil {
		glog.Error("couldn't prepare:", err)
		ctx.Fail("couldn't prepare")
		return
	}
	resp.Instance = pb.Uint64(uint64(inst))
	ctx.Return(&resp)
}

type ProtoChange proto.Change

func (c ProtoChange) Serialize() []byte {
	return proto.Change(c).Change
}

func (srv *RpcServer) handleAccept(ctx *rpcsrv.Context) {
	var req proto.AcceptRequest
	var resp proto.GenericResponse

	if err := ctx.GetArgument(&req); err != nil {
		glog.Error("Couldn't parse accept request:", err)
		ctx.Fail("couldn't parse request")
		return
	}
	inner, ok := srv.participants[req.GetCluster()]
	if !ok {
		glog.Error("bad cluster", req.GetCluster())
		ctx.Fail("bad cluster")
		return
	}

	glog.Info("server: accept:", req.String(), " by ", ctx.GetClientId())
	changes := make([]con.Change, len(req.GetChanges()))
	for i, c := range req.GetChanges() {
		changes[i] = ChangeDeserializer{}.Deserialize(c.GetChange())
	}
	acc, err := inner.Accept(con.InstanceNumber(req.GetVersion().GetInstance()), con.SequenceNumber(req.GetVersion().GetSequence()),
		changes)
	if err != nil {
		glog.Error("couldn't accept:", err)
		ctx.Fail("couldn't accept")
		return
	}
	resp.Accepted = pb.Bool(acc)
	ctx.Return(&resp)
}
func (srv *RpcServer) handleAddMember(ctx *rpcsrv.Context) {
	var req proto.AddMemberRequest
	var resp proto.GenericResponse

	if err := ctx.GetArgument(&req); err != nil {
		glog.Error("Couldn't parse addmember request:", err)
		ctx.Fail("couldn't parse request")
		return
	}
	inner, ok := srv.participants[req.GetCluster()]
	if !ok {
		glog.Error("bad cluster", req.GetCluster())
		ctx.Fail("bad cluster")
		return
	}

	glog.Info("server: addmember:", req.String(), " by ", ctx.GetClientId())
	err := inner.AddMember(con.InstanceNumber(req.GetVersion().GetInstance()), con.SequenceNumber(req.GetVersion().GetSequence()),
		con.Member{Address: req.GetMember().GetAddress()})
	if err != nil {
		glog.Error("couldn't add member:", err)
		ctx.Fail("couldn't add member")
		return
	}
	resp.Accepted = pb.Bool(true)
	ctx.Return(&resp)
}
func (srv *RpcServer) handleRmMember(ctx *rpcsrv.Context) {
	var req proto.RemoveMemberRequest
	var resp proto.GenericResponse

	if err := ctx.GetArgument(&req); err != nil {
		glog.Error("Couldn't parse rmmember request:", err)
		ctx.Fail("couldn't parse request")
		return
	}
	inner, ok := srv.participants[req.GetCluster()]
	if !ok {
		glog.Error("bad cluster", req.GetCluster())
		ctx.Fail("bad cluster")
		return
	}

	glog.Info("server: rmmember:", req.String(), " by ", ctx.GetClientId())
	err := inner.RemoveMember(con.InstanceNumber(req.GetVersion().GetInstance()), con.SequenceNumber(req.GetVersion().GetSequence()),
		con.Member{Address: req.GetMember().GetAddress()})
	if err != nil {
		glog.Error("couldn't remove member:", err)
		ctx.Fail("couldn't remove member")
		return
	}
	resp.Accepted = pb.Bool(true)
	ctx.Return(&resp)
}
func (srv *RpcServer) handleStart(ctx *rpcsrv.Context) {
	var req proto.StartParticipation
	var resp proto.GenericResponse

	if err := ctx.GetArgument(&req); err != nil {
		glog.Error("Couldn't parse startparticipation request:", err)
		ctx.Fail("couldn't parse request")
		return
	}

	inner, ok := srv.participants[req.GetCluster()]
	if !ok {
		glog.Error("bad cluster", req.GetCluster())
		ctx.Fail("bad cluster")
		return
	}

	participants := make([]con.Member, len(req.GetMembers()))
	for i := range req.GetMembers() {
		participants[i] = con.Member{Address: req.GetMembers()[i].GetAddress()}
	}

	glog.Info("server: start:", req.String(), " by ", ctx.GetClientId())
	err := inner.StartParticipation(con.InstanceNumber(req.GetVersion().GetInstance()),
		con.SequenceNumber(req.GetVersion().GetSequence()),
		req.GetCluster(),
		con.Member{Address: req.GetSelf().GetAddress()},
		con.Member{Address: req.GetMaster().GetAddress()},
		participants,
		req.GetSnapshot())
	if err != nil {
		glog.Error("couldn't start:", err)
		ctx.Fail("couldn't start")
		return
	}
	resp.Accepted = pb.Bool(true)
	ctx.Return(&resp)
}
func (srv *RpcServer) handleSubmit(ctx *rpcsrv.Context) {
	var req proto.SubmitRequest
	var resp proto.GenericResponse

	if err := ctx.GetArgument(&req); err != nil {
		glog.Error("Couldn't parse submit request:", err)
		ctx.Fail("couldn't parse request")
		return
	}

	inner, ok := srv.participants[req.GetCluster()]
	if !ok {
		glog.Error("bad cluster", req.GetCluster())
		ctx.Fail("bad cluster")
		return
	}

	glog.Info("server: submit: ", req.String(), " by ", ctx.GetClientId())

	changes := make([]con.Change, len(req.GetChanges()))
	for i := range req.GetChanges() {
		changes[i] = ChangeDeserializer{}.Deserialize(req.GetChanges()[i].GetChange())
	}

	err := inner.SubmitRequest(changes)
	if err != nil {
		glog.Error("server: couldn't submit: ", err)
		ctx.Fail("couldn't submit")
		return
	}
	resp.Accepted = pb.Bool(true)
	ctx.Return(&resp)
}

func (srv *RpcServer) Register(cluster string, participant *con.Participant, decoder con.ChangeDeserializer) error {
	if _, ok := srv.participants[cluster]; ok {
		return con.NewError(con.ERR_STATE, fmt.Sprintf("Server is already part of cluster %s", cluster), nil)
	}

	srv.participants[cluster] = participant
	return nil
}

func main() {
	initMaster := flag.Bool("initMaster", false, "Initialize as master, then add participants")
	participants := flag.String("participants", "", "Comma-separated list of other participants' addresses")
	host := flag.String("host", "localhost", "externally reachable host name or IP")
	port := flag.Uint("listen", 9000, "Port to listen on")
	cluster := flag.String("cluster", "cluster1", "ClusterID")
	interval := flag.Uint("interval", 2, "interval for submitting random changes")

	flag.Parse()

	CLIENT_ID = fmt.Sprintf("<client:%s:%d>", *host, *port)

	glog.Info("setting up server")
	server, err := NewRpcServer(*host, *port)
	if err != nil {
		glog.Exit("Couldn't open server:", err)
	}

	glog.Info("creating participant for", *cluster)
	participant := con.NewParticipant(*cluster, &Connector{}, State{inner: make(map[string]string)})
	participant.SetEventHandler(EventHandler{})
	server.participants[*cluster] = participant

	if *initMaster {
		glog.Info("initializing master")
		addr := fmt.Sprintf("%s:%d", *host, *port)
		participant.InitMaster(con.Member{Address: addr}, []byte{})
		for _, a := range strings.Split(*participants, ",") {
			glog.Info("Adding member ", a)
			fmt.Println("AddMember err? ", participant.AddParticipant(con.Member{Address: a}))
		}
		participant.Submit([]con.Change{})
	}

	i := 0
	for {
		time.Sleep(time.Duration(*interval) * time.Second)

		if participant.IsMaster() {
			glog.Info("<MASTER>")
		} else if err := participant.PingMaster(); err != nil {
			glog.Info("<Follower> Master down:", err)
		} else {
			glog.Info("<Follower>")
		}

		err := participant.SubmitOne(
			Change{t: change_ADD, key: fmt.Sprintf("%d.k%d", *port, i), val: fmt.Sprintf("v%d", i)})
		if err != nil {
			glog.Info("couldn't submit change:", err)
			continue
		}
		if i%5 == 0 {
			glog.Info("master? ", participant.IsMaster(), " state len: ", len(participant.GetState().(State).inner),
				" state: ", participant.GetState().(State))
		}
		i++
	}
}
