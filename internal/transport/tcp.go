package transport

import (
	"encoding/gob"
	"net"
	"time"
)

type Handler interface {
	HandleRequestVote(req RequestVoteRequest) RequestVoteResponse
	HandleAppendEntries(req AppendEntriesRequest) AppendEntriesResponse
}

func SendRPC(addr string, rpcType RPCType, req interface{}, resp interface{}) error {
	conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
	if err != nil {
		return err
	}
	defer conn.Close()

	_ = conn.SetDeadline(time.Now().Add(1 * time.Second))

	rpcReq := RPC{Type: rpcType}
	switch v := req.(type) {
	case RequestVoteRequest:
		rpcReq.RVReq = &v
	case AppendEntriesRequest:
		rpcReq.AEreq = &v
	}

	enc := gob.NewEncoder(conn)
	if err := enc.Encode(rpcReq); err != nil {
		return err
	}

	dec := gob.NewDecoder(conn)
	var rpcResp RPC
	if err := dec.Decode(&rpcResp); err != nil {
		return err
	}

	switch v := resp.(type) {
	case *RequestVoteResponse:
		if rpcResp.RVResp != nil {
			*v = *rpcResp.RVResp
		}
	case *AppendEntriesResponse:
		if rpcResp.AEresp != nil {
			*v = *rpcResp.AEresp
		}
	}
	return nil
}

func StartRPCServer(addr string, handler Handler, stopCh chan struct{}) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	go func() {
		<-stopCh
		_ = ln.Close()
	}()

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				select {
				case <-stopCh:
					return
				default:
					continue
				}
			}
			go handleConn(conn, handler)
		}
	}()
	return nil
}

func handleConn(conn net.Conn, handler Handler) {
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(1 * time.Second))

	var rpcReq RPC
	dec := gob.NewDecoder(conn)
	if err := dec.Decode(&rpcReq); err != nil {
		return
	}

	enc := gob.NewEncoder(conn)
	rpcResp := RPC{}

	switch rpcReq.Type {
	case RPCRequestVoteReq:
		if rpcReq.RVReq != nil {
			resp := handler.HandleRequestVote(*rpcReq.RVReq)
			rpcResp.Type = RPCRequestVoteResp
			rpcResp.RVResp = &resp
		}
	case RPCAppendEntriesReq:
		if rpcReq.AEreq != nil {
			resp := handler.HandleAppendEntries(*rpcReq.AEreq)
			rpcResp.Type = RPCAppendEntriesResp
			rpcResp.AEresp = &resp
		}
	}

	_ = enc.Encode(rpcResp)
}
