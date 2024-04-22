package main

import (
	"bufio"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"strings"
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

type KVStore struct {
	mu   sync.Mutex
	data map[string]string
}

func (kv *KVStore) Set(key, value string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.data[key] = value
}

func (kv *KVStore) Get(key string) (string, bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	value, ok := kv.data[key]
	return value, ok
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
	store          *KVStore
	applyCh        chan LogEntry
	shutdown       chan struct{}
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
	for {
		select {
		case <-rn.heartbeatTimer.C:
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
		case <-rn.shutdown:
			return
		}
	}
}

func (rn *RaftNode) runElectionTimer() {
	for {
		select {
		case <-rn.electionTimer.C:
			rn.mu.Lock()
			if rn.state == Follower {
				rn.becomeCandidate()
			}
			rn.mu.Unlock()
		case <-rn.shutdown:
			return
		}
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

func (rn *RaftNode) logger(format string, args ...interface{}) {
	fmt.Printf("[Node %d] %s\n", rn.id, fmt.Sprintf(format, args...))
}

func (rn *RaftNode) becomeFollower(term int) {
	rn.currentTerm = term
	rn.votedFor = -1
	rn.state = Follower
	rn.electionTimer.Reset(ElectionTimeout)
	rn.logger("Became follower in term %d", term)
}

func (rn *RaftNode) becomeCandidate() {
	rn.currentTerm++
	rn.votedFor = rn.id
	rn.state = Candidate
	rn.votes = 1
	rn.broadcastRequestVote()
	rn.electionTimer.Reset(ElectionTimeout)
	rn.logger("Became candidate in term %d", rn.currentTerm)
}

func (rn *RaftNode) becomeLeader() {
	rn.state = Leader
	rn.heartbeatTimer = time.NewTimer(HeartbeatInterval)
	go rn.sendHeartbeats()
	rn.logger("Became leader in term %d", rn.currentTerm)
}

func (rn *RaftNode) applyCommand(command string) {
	parts := strings.Split(command, " ")
	if len(parts) == 3 && parts[0] == "set" {
		rn.store.Set(parts[1], parts[2])
		rn.logger("Applied command: %s", command)
	}
}

func (rn *RaftNode) startApplier() {
	for {
		select {
		case entry := <-rn.applyCh:
			rn.mu.Lock()
			if rn.state == Leader {
				rn.log = append(rn.log, entry)
				rn.mu.Unlock()
				rn.replicateLog(entry)
			} else {
				rn.mu.Unlock()
			}
		case <-rn.shutdown:
			return
		}
	}
}

func (rn *RaftNode) replicateLog(entry LogEntry) {
	rn.mu.Lock()
	if rn.state != Leader {
		rn.mu.Unlock()
		return
	}
	prevLogIndex := len(rn.log) - 2
	prevLogTerm := 0
	if prevLogIndex >= 0 {
		prevLogTerm = rn.log[prevLogIndex].Term
	}
	currentTerm := rn.currentTerm
	commitIndex := rn.commitIndex
	rn.mu.Unlock()

	var wg sync.WaitGroup
	majority := len(rn.peers)/2 + 1
	successCount := 1

	for _, peer := range rn.peers {
		wg.Add(1)
		go func(peer string) {
			defer wg.Done()
			args := &AppendEntriesArgs{
				Term:         currentTerm,
				LeaderID:     rn.id,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      []LogEntry{entry},
				LeaderCommit: commitIndex,
			}

			reply := &AppendEntriesReply{}
			client, err := rpc.Dial("tcp", peer)
			if err != nil {
				return
			}
			defer client.Close()

			client.Call("RaftNode.AppendEntries", args, reply)
			rn.mu.Lock()
			defer rn.mu.Unlock()

			if reply.Term > currentTerm {
				rn.becomeFollower(reply.Term)
				return
			}

			if reply.Success {
				successCount++
				if successCount >= majority && rn.state == Leader {
					rn.commitIndex = len(rn.log) - 1
					rn.lastApplied = rn.commitIndex
					go rn.applyCommittedLogs()
				}
			}
		}(peer)
	}

	wg.Wait()
}

func (rn *RaftNode) startClient() {
	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Printf("[Node %d] Enter a command (set <key> <value>) or 'crash' to crash the leader: ", rn.id)
		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(input)

		if input == "quit" {
			fmt.Printf("[Node %d] Shutting down\n", rn.id)
			close(rn.shutdown)
			return
		}

		if input == "crash" {
			rn.mu.Lock()
			if rn.state == Leader {
				rn.crash()
			} else {
				fmt.Printf("[Node %d] Cannot crash. Not the leader.\n", rn.id)
			}
			rn.mu.Unlock()
			continue
		}

		rn.mu.Lock()
		if rn.state == Leader {
			entry := LogEntry{Term: rn.currentTerm, Command: input}
			rn.log = append(rn.log, entry)
			rn.mu.Unlock()
			go rn.replicateLog(entry)
			fmt.Printf("[Node %d] Appended command to log: %s\n", rn.id, input)
		} else {
			rn.mu.Unlock()
			fmt.Printf("[Node %d] Not the leader. Cannot process command.\n", rn.id)
		}
	}
}

func (rn *RaftNode) redirectToLeader(leaderID int, command string) {
	leaderAddr := rn.peers[leaderID]
	client, err := rpc.Dial("tcp", leaderAddr)
	if err != nil {
		fmt.Printf("[Node %d] Failed to connect to leader: %v\n", rn.id, err)
		return
	}
	defer client.Close()

	var reply struct{}
	err = client.Call("RaftNode.RedirectCommand", command, &reply)
	if err != nil {
		fmt.Printf("[Node %d] Failed to redirect command to leader: %v\n", rn.id, err)
	}
}

func (rn *RaftNode) RedirectCommand(command string, reply *struct{}) error {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if rn.state == Leader {
		entry := LogEntry{Term: rn.currentTerm, Command: command}
		rn.applyCh <- entry
		fmt.Printf("[Node %d] Received redirected command: %s\n", rn.id, command)
	} else {
		fmt.Printf("[Node %d] Received redirected command but not the leader\n", rn.id)
	}

	return nil
}

func (rn *RaftNode) applyCommittedLogs() {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	for rn.lastApplied < rn.commitIndex {
		rn.lastApplied++
		entry := rn.log[rn.lastApplied]
		rn.applyCommand(entry.Command)
	}
}

func (rn *RaftNode) crash() {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	rn.logger("Crashing...")
	rn.state = Follower
	rn.electionTimer.Reset(ElectionTimeout + time.Duration(rand.Intn(100))*time.Millisecond)
}

func main() {
	rand.Seed(time.Now().UnixNano())

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
			store:       &KVStore{data: make(map[string]string)},
			applyCh:     make(chan LogEntry),
			shutdown:    make(chan struct{}),
		}
		nodes[i].start()
		go nodes[i].startApplier()
	}

	for i := 0; i < 5; i++ {
		go nodes[i].startClient()
	}

	select {}
}
