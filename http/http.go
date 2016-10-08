package http

import (
	"bytes"
	con "clusterconsensus"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

const (
	method_PREPARE             string = "prepare"
	method_ACCEPT                     = "accept"
	method_ADDMEMBER                  = "addmember"
	method_RMMEMBER                   = "rmmember"
	method_START_PARTICIPATION        = "start"
	method_SUBMIT                     = "submit"
)

// Implements transport and encoding via HTTP/JSON for clusterconsensus.
// Change and State implementations are still not provided, however.

// Implements Connector
type HttpConnector struct {
	client *http.Client
}

func NewHttpConnector(timeout time.Duration) HttpConnector {
	cl := &http.Client{Timeout: timeout}
	return HttpConnector{client: cl}
}

func (c HttpConnector) Connect(clusterId string, m con.Member) (con.ConsensusClient, error) {
	return HttpTransport{client: c.client, peerUrl: m.Address, clusterId: clusterId}, nil
}

// Implements ConsensusClient
type HttpTransport struct {
	client    *http.Client
	peerUrl   string
	clusterId string
}

func (t HttpTransport) buildUrl(method string) string {
	return fmt.Sprintf("%s/_clusterc/%s/%s", t.peerUrl, t.clusterId, method)
}

// Roundtrips a POST request. Also managed serialization, deserialization, and error handling
func (t HttpTransport) postRequest(body []byte, method string, target interface{}) error {
	path := t.buildUrl(method)

	response, err := t.client.Post(path, "application/json", bytes.NewReader(body))

	if err != nil {
		return con.NewError(con.ERR_CALL, "HTTP Error", err)
	}

	defer response.Body.Close()

	if response.StatusCode != 200 {
		return con.NewError(con.ERR_CALL, fmt.Sprintf("Received HTTP code %d", response.StatusCode), err)
	}

	blob := bytes.NewBuffer(nil)
	_, err = blob.ReadFrom(response.Body)

	if err != nil {
		return con.NewError(con.ERR_IO, "Couldn't read from body", err)
	}

	err = json.Unmarshal(blob.Bytes(), target)

	if err != nil {
		return con.NewError(con.ERR_ENCODING, "Couldn't decode body", err)
	}

	return nil
}

func (t HttpTransport) Close() error {
	return nil
}

func (t HttpTransport) Prepare(i con.InstanceNumber, m con.Member) (con.InstanceNumber, error) {
	body, err := json.Marshal(PrepareRequest{Instance: uint64(i), Master: JSONAddress{Addr: m.Address}})

	if err != nil {
		return 0, con.NewError(con.ERR_ENCODING, "JSON Encoding error", err)
	}

	var decoded PrepareResponse

	if err := t.postRequest(body, method_PREPARE, &decoded); err != nil {
		return 0, err
	}

	return con.InstanceNumber(decoded.Accepted), nil
}

func (t HttpTransport) Accept(i con.InstanceNumber, s con.SequenceNumber, c []con.Change) (bool, error) {
	changes := make([][]byte, len(c))

	for i := range c {
		changes[i] = c[i].Serialize()
	}

	body, err := json.Marshal(AcceptRequest{Instance: uint64(i), Sequence: uint64(s), Changes: changes})

	if err != nil {
		return false, con.NewError(con.ERR_ENCODING, "JSON Encoding error", err)
	}

	var decoded GenericResponse

	if err := t.postRequest(body, method_ACCEPT, &decoded); err != nil {
		return false, err
	}

	if !decoded.Accepted {
		return false, decoded.Err.ToError()
	}

	return true, nil
}

func (t HttpTransport) AddMember(i con.InstanceNumber, s con.SequenceNumber, m con.Member) error {
	body, err := json.Marshal(ChangeMemberRequest{Instance: uint64(i), Sequence: uint64(s), Mem: JSONAddress{Addr: m.Address}})

	if err != nil {
		return con.NewError(con.ERR_ENCODING, "JSON Encoding error", err)
	}

	var decoded GenericResponse

	if err := t.postRequest(body, method_ADDMEMBER, &decoded); err != nil {
		return err
	}

	if !decoded.Accepted {
		return decoded.Err.ToError()
	}

	return nil
}

func (t HttpTransport) RemoveMember(i con.InstanceNumber, s con.SequenceNumber, m con.Member) error {
	body, err := json.Marshal(ChangeMemberRequest{Instance: uint64(i), Sequence: uint64(s), Mem: JSONAddress{Addr: m.Address}})

	if err != nil {
		return con.NewError(con.ERR_ENCODING, "JSON Encoding error", err)
	}

	var decoded GenericResponse

	if err := t.postRequest(body, method_RMMEMBER, &decoded); err != nil {
		return err
	}

	if !decoded.Accepted {
		return decoded.Err.ToError()
	}

	return nil
}

func (t HttpTransport) StartParticipation(i con.InstanceNumber,
	s con.SequenceNumber,
	cluster string,
	self con.Member,
	master con.Member,
	members []con.Member,
	snapshot []byte) error {
	participants := make([]JSONAddress, len(members))

	for i := range members {
		participants[i] = JSONAddress{Addr: members[i].Address}
	}

	request := StartParticipationRequest{Instance: uint64(i),
		Sequence:     uint64(s),
		Cluster:      cluster,
		Self:         JSONAddress{Addr: self.Address},
		Master:       JSONAddress{Addr: master.Address},
		Participants: participants,
		Snapshot:     snapshot}

	body, err := json.Marshal(request)

	if err != nil {
		return con.NewError(con.ERR_ENCODING, "JSON Encoding error", err)
	}

	var decoded GenericResponse

	if err := t.postRequest(body, method_START_PARTICIPATION, &decoded); err != nil {
		return err
	}

	if !decoded.Accepted {
		return decoded.Err.ToError()
	}

	return nil
}

func (t HttpTransport) SubmitRequest(c []con.Change) error {
	changes := make([][]byte, len(c))

	for i := range c {
		changes[i] = c[i].Serialize()
	}

	body, err := json.Marshal(SubmitRequest{Changes: changes})

	if err != nil {
		return con.NewError(con.ERR_ENCODING, "JSON Encoding error", err)
	}

	var decoded GenericResponse

	if err := t.postRequest(body, method_SUBMIT, &decoded); err != nil {
		return err
	}

	if !decoded.Accepted {
		return decoded.Err.ToError()
	}

	return nil
}
