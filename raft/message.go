package raft

type MsgType int8

const (
	VoteRequest MsgType = iota
	VoteResponse
	Heartbeat
	HeartbeatResponse
)

type Message struct {
	Type         MsgType
	From         uint64
	To           uint64
	Term         uint64
	LastLogIndex uint64
	LastLogTerm  uint64
	Reject       bool
}
