package http

import con "clusterconsensus"

type JSONAddress struct {
	Addr string
}

type JSONErr struct {
	Code string
	Err  string
}

func FromError(e con.ConsensusError) JSONErr {
	return JSONErr{Code: e.Code(), Err: e.Error()}
}

func (je JSONErr) ToError() error {
	return con.NewError(je.Code, je.Err, nil)
}

type PrepareRequest struct {
	Instance uint64
	Master   JSONAddress
}

type PrepareResponse struct {
	Accepted uint64
	Err      JSONErr
}

type AcceptRequest struct {
	Instance uint64
	Sequence uint64
	Changes  [][]byte
}

type GenericResponse struct {
	Accepted bool
	Err      JSONErr
}

// Used for both /addmember and /rmmember
type ChangeMemberRequest struct {
	Instance uint64
	Sequence uint64
	Mem      JSONAddress
}

type StartParticipationRequest struct {
	Instance     uint64
	Sequence     uint64
	Cluster      string
	Self         JSONAddress // the new participant (not the sender)
	Master       JSONAddress
	Participants []JSONAddress
	Snapshot     []byte
} // response is GenericResponse

type SubmitRequest struct {
	Changes [][]byte
} // response is GenericResponse
