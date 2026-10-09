package raft

import (
	"math/rand/v2"
	"testing"
)

// newTestNode makes one node with a fixed seed, for single-node tests.
func newTestNode(id uint64, peers []uint64) *Node {
	return NewNode(id, peers, rand.New(rand.NewPCG(1, id)))
}

// ---------- single node, no simulator ----------

func TestSingleNodeBecomesLeader(t *testing.T) {
	n := newTestNode(1, nil)
	for i := 0; i < 20; i++ { // timeout is at most 19, so 20 ticks is enough
		n.Tick()
	}
	if n.role != Leader {
		t.Fatalf("expected Leader, got role %d", n.role)
	}
	if n.currentTerm != 1 || n.leader != 1 {
		t.Fatalf("expected term 1 leader 1, got term %d leader %d", n.currentTerm, n.leader)
	}
}

func TestElectionTimeoutIsRandomInRange(t *testing.T) {
	n := newTestNode(1, []uint64{2, 3})
	seen := make(map[int]bool)
	for i := 0; i < 1000; i++ {
		n.becomeFollower(n.currentTerm, 0)
		if n.electionTimeout < 10 || n.electionTimeout >= 20 {
			t.Fatalf("timeout %d outside [10, 20)", n.electionTimeout)
		}
		seen[n.electionTimeout] = true
	}
	if len(seen) < 5 {
		t.Fatalf("timeouts not random enough, only saw %d distinct values", len(seen))
	}
}

func TestFollowerGrantsVote(t *testing.T) {
	n := newTestNode(2, []uint64{1, 3})
	n.Step(Message{Type: VoteRequest, From: 1, To: 2, Term: 1})

	out := n.ReadMessages()
	if len(out) != 1 {
		t.Fatalf("expected 1 message, got %d", len(out))
	}
	m := out[0]
	if m.Type != VoteResponse || m.To != 1 || m.Reject {
		t.Fatalf("expected granted VoteResponse to 1, got %+v", m)
	}
	if n.votedFor != 1 || n.currentTerm != 1 {
		t.Fatalf("expected votedFor 1 term 1, got votedFor %d term %d", n.votedFor, n.currentTerm)
	}
}

func TestVoteOncePerTerm(t *testing.T) {
	n := newTestNode(2, []uint64{1, 3})
	n.Step(Message{Type: VoteRequest, From: 1, To: 2, Term: 5})
	n.ReadMessages()

	// a different candidate in the same term must be refused
	n.Step(Message{Type: VoteRequest, From: 3, To: 2, Term: 5})
	out := n.ReadMessages()
	if len(out) != 1 || !out[0].Reject {
		t.Fatalf("expected rejection for second candidate, got %+v", out)
	}
	if n.votedFor != 1 {
		t.Fatalf("votedFor changed to %d", n.votedFor)
	}

	// the same candidate asking again (a duplicate) gets yes again
	n.Step(Message{Type: VoteRequest, From: 1, To: 2, Term: 5})
	out = n.ReadMessages()
	if len(out) != 1 || out[0].Reject {
		t.Fatalf("expected duplicate request to be granted, got %+v", out)
	}
}

func TestStaleMessageIgnored(t *testing.T) {
	n := newTestNode(2, []uint64{1, 3})
	n.currentTerm = 5
	n.Step(Message{Type: VoteRequest, From: 1, To: 2, Term: 3})
	n.Step(Message{Type: Heartbeat, From: 1, To: 2, Term: 3})

	if n.currentTerm != 5 || n.role != Follower || n.votedFor != 0 || n.leader != 0 {
		t.Fatalf("stale messages changed state: term %d role %d votedFor %d leader %d",
			n.currentTerm, n.role, n.votedFor, n.leader)
	}
	if out := n.ReadMessages(); len(out) != 0 {
		t.Fatalf("expected no replies to stale messages, got %+v", out)
	}
}

func TestLeaderStepsDownOnHigherTerm(t *testing.T) {
	n := newTestNode(1, []uint64{2, 3})
	n.role, n.currentTerm, n.leader = Leader, 3, 1

	n.Step(Message{Type: Heartbeat, From: 2, To: 1, Term: 4})

	if n.role != Follower || n.currentTerm != 4 || n.leader != 2 || n.votedFor != 0 {
		t.Fatalf("expected follower term 4 leader 2, got role %d term %d leader %d votedFor %d",
			n.role, n.currentTerm, n.leader, n.votedFor)
	}
}

func TestCandidateStepsDownOnSameTermHeartbeat(t *testing.T) {
	n := newTestNode(1, []uint64{2, 3})
	n.startElection() // now candidate in term 1
	n.ReadMessages()

	n.Step(Message{Type: Heartbeat, From: 2, To: 1, Term: 1})

	if n.role != Follower || n.leader != 2 || n.currentTerm != 1 {
		t.Fatalf("expected follower of 2 in term 1, got role %d leader %d term %d",
			n.role, n.leader, n.currentTerm)
	}
}

func TestRejectedVotesDoNotCount(t *testing.T) {
	n := newTestNode(1, []uint64{2, 3})
	n.startElection()
	n.ReadMessages()

	n.Step(Message{Type: VoteResponse, From: 2, To: 1, Term: 1, Reject: true})
	if n.role == Leader {
		t.Fatal("became leader from a rejected vote")
	}
	n.Step(Message{Type: VoteResponse, From: 3, To: 1, Term: 1, Reject: false})
	if n.role != Leader {
		t.Fatalf("expected Leader after 2 of 3 votes, got role %d", n.role)
	}
}

func TestVoteRefusedIfCandidateLogBehind(t *testing.T) {
	// voter's last entry is (term 2, index 3)
	n := newTestNode(2, []uint64{1, 3})
	n.lastLogTerm, n.lastLogIndex = 2, 3

	// longer log but older last term: refuse (last term wins over length)
	n.Step(Message{Type: VoteRequest, From: 1, To: 2, Term: 5, LastLogTerm: 1, LastLogIndex: 10})
	if out := n.ReadMessages(); len(out) != 1 || !out[0].Reject {
		t.Fatalf("expected rejection, got %+v", out)
	}

	// same last term, shorter log: refuse
	n.Step(Message{Type: VoteRequest, From: 1, To: 2, Term: 5, LastLogTerm: 2, LastLogIndex: 2})
	if out := n.ReadMessages(); len(out) != 1 || !out[0].Reject {
		t.Fatalf("expected rejection, got %+v", out)
	}

	// same last term, equal length: grant
	n.Step(Message{Type: VoteRequest, From: 3, To: 2, Term: 5, LastLogTerm: 2, LastLogIndex: 3})
	if out := n.ReadMessages(); len(out) != 1 || out[0].Reject {
		t.Fatalf("expected grant, got %+v", out)
	}
}

// ---------- full cluster, using the simulator ----------

func TestNoTicksNoLeader(t *testing.T) {
	nw := newNetwork(t, 1, 3)
	nw.deliverAll()
	if l := nw.currentLeaders(); len(l) != 0 {
		t.Fatalf("expected no leader without ticks, got %v", l)
	}
}

func TestThreeNodesElectOneLeader(t *testing.T) {
	nw := newNetwork(t, 1, 3)
	leader := nw.tickUntilLeader(100)

	for _, id := range nw.ids {
		n := nw.nodes[id]
		if n.leader != leader {
			t.Errorf("node %d thinks leader is %d, want %d", id, n.leader, leader)
		}
		if id != leader && n.role != Follower {
			t.Errorf("node %d should be Follower, got role %d", id, n.role)
		}
	}
}

func TestLeaderStaysLeader(t *testing.T) {
	nw := newNetwork(t, 1, 3)
	leader := nw.tickUntilLeader(100)
	term := nw.nodes[leader].currentTerm

	nw.tick(200) // heartbeats should stop any new election

	if l := nw.currentLeaders(); len(l) != 1 || l[0] != leader {
		t.Fatalf("leader changed from %d to %v", leader, l)
	}
	if nw.nodes[leader].currentTerm != term {
		t.Fatalf("term changed from %d to %d", term, nw.nodes[leader].currentTerm)
	}
}

func TestIsolatedLeaderIsReplaced(t *testing.T) {
	nw := newNetwork(t, 1, 3)
	oldLeader := nw.tickUntilLeader(100)
	oldTerm := nw.nodes[oldLeader].currentTerm

	nw.isolate(oldLeader)
	newLeader := nw.tickUntilLeader(100)

	if newLeader == oldLeader {
		t.Fatal("isolated node is still the only leader")
	}
	if nw.nodes[newLeader].currentTerm <= oldTerm {
		t.Fatalf("new leader should have a higher term than %d", oldTerm)
	}
	// the old leader can't hear anyone, so it still believes it leads
	if nw.nodes[oldLeader].role != Leader {
		t.Fatal("isolated old leader should not know it was replaced yet")
	}

	nw.heal()
	nw.tick(5)

	old := nw.nodes[oldLeader]
	if old.role != Follower || old.leader != newLeader {
		t.Fatalf("after heal, old leader should follow %d, got role %d leader %d",
			newLeader, old.role, old.leader)
	}
	if l := nw.currentLeaders(); len(l) != 1 {
		t.Fatalf("expected exactly one leader after heal, got %v", l)
	}
}

func TestMinorityCannotElect(t *testing.T) {
	nw := newNetwork(t, 1, 5)
	nw.isolate(3)
	nw.isolate(4)
	nw.isolate(5)

	nw.tick(200)

	if l := nw.currentLeaders(); len(l) != 0 {
		t.Fatalf("2 of 5 nodes elected a leader: %v", l)
	}

	nw.heal()
	nw.tickUntilLeader(200)
}

// The big one: random drops and partitions, many seeds. The simulator's
// checkElectionSafety fails the test the moment two leaders share a term.
func TestChaosElectionSafety(t *testing.T) {
	for seed := uint64(0); seed < 200; seed++ {
		nw := newNetwork(t, seed, 5)
		nw.dropRate = 0.1
		chaos := rand.New(rand.NewPCG(seed, 999))

		for round := 0; round < 10; round++ {
			nw.tick(50)
			clear(nw.isolated)
			nw.isolate(uint64(chaos.IntN(5) + 1)) // cut off a random node
		}

		nw.heal()
		nw.tickUntilLeader(200)
	}
}
