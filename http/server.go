package http

import (
	"github.com/golang/glog"

	"bytes"
	con "clusterconsensus"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
)

// Implements the server side; including clusterconsensus.Server
//
// This basically means dispatching incoming requests to the right participant,
// and deserializing requests.

// Implements clusterconsensus.Server and http.Handler (just register in an HTTP server)
type HttpConsensusServer struct {
	participants map[string]con.ConsensusServer
	mux          *http.ServeMux
}

func NewHttpConsensusServer() HttpConsensusServer {
	return HttpConsensusServer{participants: make(map[string]con.ConsensusServer), mux: http.NewServeMux()}
}

func (srv HttpConsensusServer) Register(cluster string, stub con.ConsensusServer, decoder con.ChangeDeserializer) error {
	if _, ok := srv.participants[cluster]; ok {
		return con.NewError(con.ERR_STATE, fmt.Sprintf("Server is already part of cluster %s", cluster), nil)
	}

	srv.participants[cluster] = stub
	srv.mux.Handle(fmt.Sprintf("/_clusterc/%s/", cluster),
		ParticipantHandler{inner: stub, cluster: cluster, changeDecoder: decoder})

	return nil
}

func (srv HttpConsensusServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	srv.mux.ServeHTTP(w, r)
}

// Handles requests to a single participant
type ParticipantHandler struct {
	inner         con.ConsensusServer
	cluster       string
	changeDecoder con.ChangeDeserializer
}

func (h ParticipantHandler) methodFromPath(path string) string {
	base := "/_clusterc/" + h.cluster + "/"

	if !strings.HasPrefix(path, base) {
		return ""
	}

	if strings.HasSuffix(path, method_PREPARE) {
		return method_PREPARE
	} else if strings.HasSuffix(path, method_ACCEPT) {
		return method_ACCEPT
	} else if strings.HasSuffix(path, method_ADDMEMBER) {
		return method_ADDMEMBER
	} else if strings.HasSuffix(path, method_RMMEMBER) {
		return method_RMMEMBER
	} else if strings.HasSuffix(path, method_START_PARTICIPATION) {
		return method_START_PARTICIPATION
	} else if strings.HasSuffix(path, method_SUBMIT) {
		return method_SUBMIT
	} else {
		return ""
	}
}

func (h ParticipantHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		w.WriteHeader(400)
		w.Write([]byte{})
		return
	}
	defer r.Body.Close()

	method := h.methodFromPath(r.URL.Path)

	if method == "" {
		w.WriteHeader(400)
		w.Write([]byte{})
	}

	switch method {
	case method_PREPARE:
		h.handlePrepare(w, r)
	case method_ACCEPT:
		h.handleAccept(w, r)
	case method_ADDMEMBER:
		h.handleAddMember(w, r)
	case method_RMMEMBER:
		h.handleRmMember(w, r)
	case method_START_PARTICIPATION:
		h.handleStart(w, r)
	case method_SUBMIT:
		h.handleSubmit(w, r)
	}
}

func (h ParticipantHandler) parseRequest(target interface{}, r *http.Request) error {
	body := bytes.NewBuffer(nil)
	n, err := body.ReadFrom(r.Body)
	r.Body.Close()

	if err != nil || n == 0 {
		return con.NewError(con.ERR_IO, "Couldn't read request", err)
	}

	err = json.Unmarshal(body.Bytes(), target)

	if err != nil {
		return con.NewError(con.ERR_ENCODING, "Couldn't decode body", err)
	}

	return nil
}

func (h ParticipantHandler) sendError(e con.ConsensusError, w http.ResponseWriter) {
	w.WriteHeader(500)

	j, err := json.Marshal(FromError(e))

	if err != nil {
		w.Write([]byte{})
	} else {
		w.Write(j)
	}
}

func (h ParticipantHandler) sendResponse(r interface{}, w http.ResponseWriter) {
	j, err := json.Marshal(r)

	if err != nil {
		w.WriteHeader(500)
		w.Write([]byte{})
	} else {
		w.WriteHeader(200)
		w.Write(j)
	}
}

func (h ParticipantHandler) handlePrepare(w http.ResponseWriter, r *http.Request) {
	var decoded PrepareRequest

	if err := h.parseRequest(&decoded, r); err != nil {
		h.sendError(err.(con.ConsensusError), w)
		return
	}
	glog.Info("server: accept:", r.URL.Path, decoded)

	inst, err := h.inner.Prepare(con.InstanceNumber(decoded.Instance), con.Member{Address: decoded.Master.Addr})
	var result PrepareResponse

	if err != nil {
		result = PrepareResponse{Accepted: uint64(inst), Err: FromError(err.(con.ConsensusError))}
	} else {
		result = PrepareResponse{Accepted: uint64(inst)}
	}

	h.sendResponse(result, w)
}

func (h ParticipantHandler) handleAccept(w http.ResponseWriter, r *http.Request) {
	var decoded AcceptRequest

	if err := h.parseRequest(&decoded, r); err != nil {
		h.sendError(err.(con.ConsensusError), w)
		return
	}
	glog.Info("server: accept:", r.URL.Path, decoded)

	changes := make([]con.Change, len(decoded.Changes))

	for i := range decoded.Changes {
		changes[i] = h.changeDecoder.Deserialize(decoded.Changes[i])
	}

	accepted, err := h.inner.Accept(con.InstanceNumber(decoded.Instance), con.SequenceNumber(decoded.Sequence), changes)

	var result GenericResponse

	if err != nil {
		result = GenericResponse{Accepted: accepted, Err: FromError(err.(con.ConsensusError))}
	} else {
		result = GenericResponse{Accepted: accepted}
	}

	h.sendResponse(result, w)
}

func (h ParticipantHandler) handleAddMember(w http.ResponseWriter, r *http.Request) {
	var decoded ChangeMemberRequest

	if err := h.parseRequest(&decoded, r); err != nil {
		h.sendError(err.(con.ConsensusError), w)
		return
	}
	glog.Info("server: add_member:", r.URL.Path, decoded)

	err := h.inner.AddMember(con.InstanceNumber(decoded.Instance), con.SequenceNumber(decoded.Sequence), con.Member{Address: decoded.Mem.Addr})

	var result GenericResponse

	if err != nil {
		result = GenericResponse{Accepted: false, Err: FromError(err.(con.ConsensusError))}
	} else {
		result = GenericResponse{Accepted: true}
	}

	h.sendResponse(result, w)
}

func (h ParticipantHandler) handleRmMember(w http.ResponseWriter, r *http.Request) {
	var decoded ChangeMemberRequest

	if err := h.parseRequest(&decoded, r); err != nil {
		h.sendError(err.(con.ConsensusError), w)
		return
	}

	glog.Info("server: rm_member:", r.URL.Path, decoded)
	err := h.inner.RemoveMember(con.InstanceNumber(decoded.Instance), con.SequenceNumber(decoded.Sequence), con.Member{Address: decoded.Mem.Addr})

	var result GenericResponse

	if err != nil {
		result = GenericResponse{Accepted: false, Err: FromError(err.(con.ConsensusError))}
	} else {
		result = GenericResponse{Accepted: true}
	}

	h.sendResponse(result, w)
}

func (h ParticipantHandler) handleStart(w http.ResponseWriter, r *http.Request) {
	var decoded StartParticipationRequest

	if err := h.parseRequest(&decoded, r); err != nil {
		h.sendError(err.(con.ConsensusError), w)
		return
	}

	glog.Info("server: start:", r.URL.Path, decoded)
	participants := make([]con.Member, len(decoded.Participants))

	for i := range decoded.Participants {
		participants[i] = con.Member{Address: decoded.Participants[i].Addr}
	}

	err := h.inner.StartParticipation(con.InstanceNumber(decoded.Instance),
		con.SequenceNumber(decoded.Sequence),
		decoded.Cluster,
		con.Member{Address: decoded.Self.Addr},
		con.Member{Address: decoded.Master.Addr},
		participants,
		decoded.Snapshot)

	var result GenericResponse

	if err != nil {
		result = GenericResponse{Accepted: false, Err: FromError(err.(con.ConsensusError))}
	} else {
		result = GenericResponse{Accepted: true}
	}

	h.sendResponse(result, w)
}

func (h ParticipantHandler) handleSubmit(w http.ResponseWriter, r *http.Request) {
	var decoded SubmitRequest

	if err := h.parseRequest(&decoded, r); err != nil {
		h.sendError(err.(con.ConsensusError), w)
		return
	}

	glog.Info("server: submit:", r.URL.Path, decoded)
	changes := make([]con.Change, len(decoded.Changes))

	for i := range decoded.Changes {
		changes[i] = h.changeDecoder.Deserialize(decoded.Changes[i])
	}

	err := h.inner.SubmitRequest(changes)

	var result GenericResponse

	if err != nil {
		result = GenericResponse{Accepted: false, Err: FromError(err.(con.ConsensusError))}
	} else {
		result = GenericResponse{Accepted: true}
	}

	h.sendResponse(result, w)
}
