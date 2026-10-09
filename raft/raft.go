package raft

import "math/rand/v2"

type Role int8

const (
	Follower Role = iota
	Candidate
	Leader
)

type Node struct {
	id               uint64
	peers            []uint64
	role             Role
	currentTerm      uint64
	votedFor         uint64
	leader           uint64
	votes            map[uint64]bool
	electionElapsed  int
	electionTimeout  int
	heartbeatElapsed int
	heartbeatTimeout int
	lastLogIndex     uint64
	lastLogTerm      uint64
	outbox           []Message
	rng              *rand.Rand
}

func NewNode(id uint64, peers []uint64, rng *rand.Rand) *Node {
	return &Node{
		id:               id,
		peers:            peers,
		role:             Follower,
		currentTerm:      0,
		votedFor:         0,
		leader:           0,
		votes:            make(map[uint64]bool),
		electionElapsed:  0,
		electionTimeout:  rng.IntN(10) + 10,
		heartbeatElapsed: 0,
		heartbeatTimeout: 1,
		lastLogIndex:     0,
		lastLogTerm:      0,
		outbox:           nil,
		rng:              rng,
	}
}

func (n *Node) Tick() {
	if n.role == Leader {
		n.heartbeatElapsed++
		if n.heartbeatElapsed >= n.heartbeatTimeout {
			n.heartbeatElapsed = 0
			n.sendHeartbeats()
		}
	} else {
		n.electionElapsed++
		if n.electionElapsed >= n.electionTimeout {
			n.startElection()
		}
	}
}

func (n *Node) Step(m Message) {
	if m.Term > n.currentTerm {
		n.becomeFollower(m.Term, 0)
	}
	if m.Term < n.currentTerm {
		return
	}

	switch m.Type {
	case VoteRequest:
		n.handleVoteRequest(m)
	case VoteResponse:
		n.handleVoteResponse(m)
	case Heartbeat:
		n.handleHeartbeat(m)
	}
}

func (n *Node) handleVoteRequest(m Message) {
	canVote := n.votedFor == 0 || n.votedFor == m.From
	logOk := m.LastLogTerm > n.lastLogTerm ||
		(m.LastLogTerm == n.lastLogTerm && m.LastLogIndex >= n.lastLogIndex)

	if canVote && logOk {
		n.votedFor = m.From
		n.electionElapsed = 0
		n.send(Message{VoteResponse, n.id, m.From, n.currentTerm, n.lastLogIndex, n.lastLogTerm, false})
	} else {
		n.send(Message{VoteResponse, n.id, m.From, n.currentTerm, n.lastLogIndex, n.lastLogTerm, true})
	}
}

func (n *Node) handleVoteResponse(m Message) {
	if n.role == Leader || n.role == Follower {
		return
	}
	n.votes[m.From] = !m.Reject
	isMajority := checkMajority(n.votes, n.peers)
	if isMajority {
		n.becomeLeader()
	}
}

func (n *Node) handleHeartbeat(m Message) {
	n.becomeFollower(m.Term, m.From)
	n.send(Message{HeartbeatResponse, n.id, m.From, n.currentTerm, n.lastLogIndex, n.lastLogTerm, false})
}

func (n *Node) ReadMessages() []Message {
	output := n.outbox
	n.outbox = nil
	return output
}

func (n *Node) startElection() {
	n.role = Candidate
	n.leader = 0
	n.currentTerm++
	n.votedFor = n.id
	clear(n.votes)
	n.votes[n.id] = true
	n.electionElapsed = 0
	n.electionTimeout = n.rng.IntN(10) + 10
	if checkMajority(n.votes, n.peers) {
		n.becomeLeader()
		return
	}
	n.sendRequestToPeers()
}

func (n *Node) send(m Message) {
	n.outbox = append(n.outbox, m)
}

func (n *Node) sendHeartbeats() {
	for _, p := range n.peers {
		n.send(Message{Heartbeat, n.id, p, n.currentTerm, n.lastLogIndex, n.lastLogTerm, false})
	}
}

func (n *Node) becomeLeader() {
	n.leader = n.id
	n.role = Leader
	n.heartbeatElapsed = 0
	n.heartbeatTimeout = 1
	n.sendHeartbeats()
}

func (n *Node) sendRequestToPeers() {
	for _, p := range n.peers {
		n.send(Message{VoteRequest, n.id, p, n.currentTerm, n.lastLogIndex, n.lastLogTerm, false})
	}
}

func (n *Node) becomeFollower(term uint64, leader uint64) {
	n.leader = leader
	n.role = Follower
	if n.currentTerm < term {
		n.votedFor = 0
	}
	n.currentTerm = term
	n.electionElapsed = 0
	n.electionTimeout = n.rng.IntN(10) + 10
}

func checkMajority(votes map[uint64]bool, peers []uint64) bool {
	number := 0
	for _, p := range peers {
		if votes[p] == true {
			number++
		}
	}
	if number+1 > (len(peers)+1)/2 {
		return true
	} else {
		return false
	}
}
