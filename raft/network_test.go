package raft

import (
	"math/rand/v2"
	"testing"
)

// network is a fake cluster used only by tests. It owns every node, plays
// the role of the clock (tick) and the wires (deliverAll), and can break
// the wires on purpose (isolate, dropRate) to simulate failures.
type network struct {
	t        *testing.T
	ids      []uint64
	nodes    map[uint64]*Node
	isolated map[uint64]bool   // nodes cut off from everyone
	dropRate float64           // chance each message is lost, 0.0 to 1.0
	rng      *rand.Rand        // decides which messages get dropped
	leaders  map[uint64]uint64 // term -> leader id, every leader ever seen
}

// newNetwork builds a cluster of size nodes with ids 1..size.
// The same seed always produces the same run.
func newNetwork(t *testing.T, seed uint64, size int) *network {
	nw := &network{
		t:        t,
		nodes:    make(map[uint64]*Node),
		isolated: make(map[uint64]bool),
		rng:      rand.New(rand.NewPCG(seed, 0)),
		leaders:  make(map[uint64]uint64),
	}
	for i := 1; i <= size; i++ {
		nw.ids = append(nw.ids, uint64(i))
	}
	for _, id := range nw.ids {
		var peers []uint64
		for _, other := range nw.ids {
			if other != id {
				peers = append(peers, other)
			}
		}
		// each node gets its own random source, seeded from the test seed + its id
		nw.nodes[id] = NewNode(id, peers, rand.New(rand.NewPCG(seed, id)))
	}
	return nw
}

func (nw *network) isolate(id uint64) { nw.isolated[id] = true }

func (nw *network) heal() {
	clear(nw.isolated)
	nw.dropRate = 0
}

// canDeliver decides whether a message makes it across the "wire".
func (nw *network) canDeliver(m Message) bool {
	if nw.isolated[m.From] || nw.isolated[m.To] {
		return false
	}
	if nw.dropRate > 0 && nw.rng.Float64() < nw.dropRate {
		return false
	}
	return true
}

// deliverAll moves messages between nodes until every outbox is empty.
// Replies create new messages, so it loops until things go quiet.
func (nw *network) deliverAll() {
	for {
		var inFlight []Message
		for _, id := range nw.ids {
			inFlight = append(inFlight, nw.nodes[id].ReadMessages()...)
		}
		if len(inFlight) == 0 {
			return
		}
		for _, m := range inFlight {
			if nw.canDeliver(m) {
				nw.nodes[m.To].Step(m)
			}
		}
	}
}

// tick advances time by k ticks. Each tick: every node ticks, then all
// messages are delivered, then the election-safety rule is checked.
func (nw *network) tick(k int) {
	for i := 0; i < k; i++ {
		for _, id := range nw.ids {
			nw.nodes[id].Tick()
		}
		nw.deliverAll()
		nw.checkElectionSafety()
	}
}

// checkElectionSafety enforces the one rule Raft must never break:
// at most one leader per term, across the whole history of the run.
func (nw *network) checkElectionSafety() {
	for _, id := range nw.ids {
		n := nw.nodes[id]
		if n.role != Leader {
			continue
		}
		if prev, ok := nw.leaders[n.currentTerm]; ok && prev != id {
			nw.t.Fatalf("two leaders in term %d: node %d and node %d", n.currentTerm, prev, id)
		}
		nw.leaders[n.currentTerm] = id
	}
}

// currentLeaders returns every node that believes it is leader right now,
// skipping isolated nodes (an isolated old leader can still believe it).
func (nw *network) currentLeaders() []uint64 {
	var out []uint64
	for _, id := range nw.ids {
		if !nw.isolated[id] && nw.nodes[id].role == Leader {
			out = append(out, id)
		}
	}
	return out
}

// tickUntilLeader ticks until exactly one connected leader exists, or fails.
func (nw *network) tickUntilLeader(maxTicks int) uint64 {
	for i := 0; i < maxTicks; i++ {
		nw.tick(1)
		if l := nw.currentLeaders(); len(l) == 1 {
			return l[0]
		}
	}
	nw.t.Fatalf("no single leader after %d ticks", maxTicks)
	return 0
}
