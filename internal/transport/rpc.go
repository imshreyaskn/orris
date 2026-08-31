package transport

import "orris/internal/storage"


type RPCType string

const (
	RPCRequestVoteReq    RPCType = "RV_REQ"
	RPCRequestVoteResp   RPCType = "RV_RESP"
	RPCAppendEntriesReq  RPCType = "AE_REQ"
	RPCAppendEntriesResp RPCType = "AE_RESP"
)

type RequestVoteRequest struct {
	Term         int
	CandidateID  string
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteResponse struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesRequest struct {
	Term         int
	LeaderID     string
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []storage.LogEntry
	LeaderCommit int
}

type AppendEntriesResponse struct {
	Term       int
	Success    bool
	MatchIndex int
}

type RPC struct {
	Type   RPCType
	RVReq  *RequestVoteRequest
	RVResp *RequestVoteResponse
	AEreq  *AppendEntriesRequest
	AEresp *AppendEntriesResponse
}
