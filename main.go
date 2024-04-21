package main

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"sync"
	"time"
)

const (
	ElectionTimeout   = 150 * time.Millisecond
	HeartbeatInterval = 50 * time.Millisecond
)

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

type LogEntry struct {
	Term    int
	Command string
}

type RaftNode struct {
	mu             sync.Mutex
	id             int
	currentTerm    int
	votedFor       int
	log            []LogEntry
	commitIndex    int
	lastApplied    int
	state          State
	electionTimer  *time.Timer
	heartbeatTimer *time.Timer
	votes          int
	peers          []string
}

type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rn *RaftNode) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if args.Term < rn.currentTerm {
		reply.Term = rn.currentTerm
		reply.VoteGranted = false
		return nil
	}

	if args.Term > rn.currentTerm {
		rn.becomeFollower(args.Term)
	}

	if rn.votedFor == -1 || rn.votedFor == args.CandidateID {
		reply.VoteGranted = true
		rn.votedFor = args.CandidateID
		rn.electionTimer.Reset(ElectionTimeout)
	}

	reply.Term = rn.currentTerm
	return nil
}

func (rn *RaftNode) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {

	rn.mu.Lock()
	defer rn.mu.Unlock()

	if args.Term < rn.currentTerm {
		reply.Term = rn.currentTerm
		reply.Success = false
		return nil
	}

	if args.Term > rn.currentTerm {
		rn.becomeFollower(args.Term)
	}

	rn.electionTimer.Reset(ElectionTimeout)

	if args.PrevLogIndex >= 0 && (args.PrevLogIndex >= len(rn.log) || rn.log[args.PrevLogIndex].Term != args.PrevLogTerm) {
		reply.Success = false
		return nil
	}

	rn.log = rn.log[:args.PrevLogIndex+1]
	rn.log = append(rn.log, args.Entries...)

	if args.LeaderCommit > rn.commitIndex {
		rn.commitIndex = min(args.LeaderCommit, len(rn.log)-1)
	}

	reply.Term = rn.currentTerm
	reply.Success = true
	return nil
}

func (rn *RaftNode) becomeFollower(term int) {
	rn.currentTerm = term
	rn.votedFor = -1
	rn.state = Follower
	rn.electionTimer.Reset(ElectionTimeout)
}

func (rn *RaftNode) becomeCandidate() {
	rn.currentTerm++
	rn.votedFor = rn.id
	rn.state = Candidate
	rn.votes = 1
	rn.broadcastRequestVote()
	rn.electionTimer.Reset(ElectionTimeout)
}

func (rn *RaftNode) becomeLeader() {
	rn.state = Leader
	rn.heartbeatTimer = time.NewTimer(HeartbeatInterval)
	go rn.sendHeartbeats()
}

func (rn *RaftNode) broadcastRequestVote() {
	lastLogIndex := len(rn.log) - 1
	lastLogTerm := 0
	if lastLogIndex >= 0 {
		lastLogTerm = rn.log[lastLogIndex].Term
	}

	args := &RequestVoteArgs{
		Term:         rn.currentTerm,
		CandidateID:  rn.id,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	for _, peer := range rn.peers {
		go func(peer string) {
			reply := &RequestVoteReply{}
			client, err := rpc.Dial("tcp", peer)
			if err != nil {
				return
			}
			defer client.Close()

			client.Call("RaftNode.RequestVote", args, reply)
			rn.mu.Lock()
			defer rn.mu.Unlock()

			if reply.Term > rn.currentTerm {
				rn.becomeFollower(reply.Term)
				return
			}

			if reply.VoteGranted {
				rn.votes++
				if rn.state == Candidate && rn.votes > len(rn.peers)/2 {
					rn.becomeLeader()
				}
			}
		}(peer)
	}
}

func (rn *RaftNode) sendHeartbeats() {
	for range rn.heartbeatTimer.C {
		rn.mu.Lock()
		if rn.state != Leader {
			rn.mu.Unlock()
			return
		}

		prevLogIndex := len(rn.log) - 1
		prevLogTerm := 0
		if prevLogIndex >= 0 {
			prevLogTerm = rn.log[prevLogIndex].Term
		}

		args := &AppendEntriesArgs{
			Term:         rn.currentTerm,
			LeaderID:     rn.id,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      []LogEntry{},
			LeaderCommit: rn.commitIndex,
		}
		rn.mu.Unlock()

		for _, peer := range rn.peers {
			go func(peer string) {
				reply := &AppendEntriesReply{}
				client, err := rpc.Dial("tcp", peer)
				if err != nil {
					return
				}
				defer client.Close()

				client.Call("RaftNode.AppendEntries", args, reply)
				rn.mu.Lock()
				defer rn.mu.Unlock()

				if reply.Term > rn.currentTerm {
					rn.becomeFollower(reply.Term)
					return
				}
			}(peer)
		}
	}
}

func (rn *RaftNode) runElectionTimer() {
	for {
		<-rn.electionTimer.C
		rn.mu.Lock()
		if rn.state == Follower {
			rn.becomeCandidate()
		}
		rn.mu.Unlock()
	}
}

func (rn *RaftNode) start() {
	rpc.Register(rn)
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", 1234+rn.id))
	if err != nil {
		log.Fatal(err)
	}
	go rpc.Accept(ln)

	rn.electionTimer = time.NewTimer(ElectionTimeout)
	go rn.runElectionTimer()
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func main() {
	nodes := make([]*RaftNode, 5)
	peers := []string{
		"localhost:1235",
		"localhost:1236",
		"localhost:1237",
		"localhost:1238",
		"localhost:1239",
	}

	for i := 0; i < 5; i++ {
		nodes[i] = &RaftNode{
			id:          i,
			currentTerm: 0,
			votedFor:    -1,
			log:         []LogEntry{},
			commitIndex: -1,
			lastApplied: -1,
			state:       Follower,
			peers:       append(peers[:i], peers[i+1:]...),
		}
		nodes[i].start()
	}

	time.Sleep(5 * time.Second)
}
