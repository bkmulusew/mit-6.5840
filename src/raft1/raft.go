package raft

// The file ../raftapi/raftapi.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// In addition,  Make() creates a new raft peer that implements the
// raft interface.

import (
	"math/rand"
	"sync"
	"time"

	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type LogEntry struct {
	Term    int
	Command interface{}
}

const (
	Follower = iota
	Candidate
	Leader
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Persistent state on all servers
	currentTerm int
	votedFor    int
	log         []LogEntry

	// Volatile state on all servers
	commitIndex int
	lastApplied int

	// Volatile state on leaders
	nextIndex  []int
	matchIndex []int

	state             int
	electionResetTime time.Time
	heartbeatInterval time.Duration

	applyCh chan raftapi.ApplyMsg
}

// return currentTerm and whether this server believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}


// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}


// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

func (rf *Raft) lastLogIndex() int {
	return len(rf.log) - 1
}

func (rf *Raft) lastLogTerm() int {
	return rf.log[len(rf.log)-1].Term
}

func (rf *Raft) lastLogIndexAndTerm() (int, int) {
	return rf.lastLogIndex(), rf.lastLogTerm()
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = Follower
		// persist later in 3C
	}

	reply.Term = rf.currentTerm

	myLastIndex, myLastTerm := rf.lastLogIndexAndTerm()
	upToDate := args.LastLogTerm > myLastTerm ||
		(args.LastLogTerm == myLastTerm && args.LastLogIndex >= myLastIndex)

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && upToDate {
		rf.votedFor = args.CandidateId
		rf.electionResetTime = time.Now()
		reply.VoteGranted = true
		// persist later in 3C
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = false
	reply.Term = rf.currentTerm
	reply.ConflictIndex = 0
	reply.ConflictTerm = -1

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}
	rf.state = Follower
	rf.electionResetTime = time.Now()
	reply.Term = rf.currentTerm

	// 1. Follower is missing PrevLogIndex.
	if args.PrevLogIndex >= len(rf.log) {
		reply.ConflictIndex = len(rf.log)
		return
	}

	// 2. PrevLogTerm mismatch.
	if args.PrevLogIndex >= 0 && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.ConflictTerm = rf.log[args.PrevLogIndex].Term

		conflictIndex := args.PrevLogIndex
		for conflictIndex > 0 && rf.log[conflictIndex-1].Term == reply.ConflictTerm {
			conflictIndex--
		}
		reply.ConflictIndex = conflictIndex
		return
	}

	// 3. Append new entries, deleting conflicting suffix first.
	insertIndex := args.PrevLogIndex + 1
	i := 0

	for i < len(args.Entries) {
		if insertIndex+i >= len(rf.log) {
			break
		}
		if rf.log[insertIndex+i].Term != args.Entries[i].Term {
			rf.log = rf.log[:insertIndex+i]
			break
		}
		i++
	}

	if i < len(args.Entries) {
		rf.log = append(rf.log, args.Entries[i:]...)
	}

	// 4. Update commit index from leader.
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.lastLogIndex())
	}

	reply.Success = true
}

// example code to send a RequestVote RPC to a server.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.peers[server].Call("Raft.AppendEntries", args, reply)
}

// the service using Raft wants to start agreement on the next command.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()

	term := rf.currentTerm
	if rf.state != Leader {
		rf.mu.Unlock()
		return -1, term, false
	}

	rf.log = append(rf.log, LogEntry{
		Term:    rf.currentTerm,
		Command: command,
	})
	index := rf.lastLogIndex()

	rf.matchIndex[rf.me] = index
	rf.nextIndex[rf.me] = index + 1

	rf.mu.Unlock()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.replicateToPeer(i, term)
	}

	return index, term, true
}

func (rf *Raft) replicateToPeer(server int, term int) {
	rf.mu.Lock()

	if rf.state != Leader || rf.currentTerm != term {
		rf.mu.Unlock()
		return
	}

	nextIdx := rf.nextIndex[server]
	prevLogIndex := nextIdx - 1
	prevLogTerm := rf.log[prevLogIndex].Term

	entries := make([]LogEntry, len(rf.log[nextIdx:]))
	copy(entries, rf.log[nextIdx:])

	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}

	rf.mu.Unlock()

	reply := &AppendEntriesReply{}
	ok := rf.sendAppendEntries(server, args, reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader || rf.currentTerm != term {
		return
	}

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = Follower
		rf.votedFor = -1
		rf.electionResetTime = time.Now()
		return
	}

	if reply.Success {
		match := args.PrevLogIndex + len(args.Entries)
		rf.matchIndex[server] = match
		rf.nextIndex[server] = match + 1
		rf.updateCommitIndex()
		return
	}

	// Backtrack nextIndex using follower conflict info.
	if reply.ConflictTerm != -1 {
		lastIndexOfTerm := -1
		for i := len(rf.log) - 1; i >= 0; i-- {
			if rf.log[i].Term == reply.ConflictTerm {
				lastIndexOfTerm = i
				break
			}
		}

		if lastIndexOfTerm != -1 {
			rf.nextIndex[server] = lastIndexOfTerm + 1
		} else {
			rf.nextIndex[server] = reply.ConflictIndex
		}
	} else {
		rf.nextIndex[server] = reply.ConflictIndex
	}

	if rf.nextIndex[server] < 1 {
		rf.nextIndex[server] = 1
	}
}

func (rf *Raft) updateCommitIndex() {
	for n := rf.lastLogIndex(); n > rf.commitIndex; n-- {
		if rf.log[n].Term != rf.currentTerm {
			continue
		}

		count := 1 // leader itself
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			if rf.matchIndex[i] >= n {
				count++
			}
		}

		if count > len(rf.peers)/2 {
			rf.commitIndex = n
			break
		}
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()

	rf.state = Candidate
	rf.currentTerm++
	termStarted := rf.currentTerm
	rf.votedFor = rf.me
	rf.electionResetTime = time.Now()

	lastLogIndex, lastLogTerm := rf.lastLogIndexAndTerm()
	votes := 1
	majority := len(rf.peers)/2 + 1

	rf.mu.Unlock()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(server int) {
			args := &RequestVoteArgs{
				Term:         termStarted,
				CandidateId:  rf.me,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			reply := &RequestVoteReply{}

			ok := rf.sendRequestVote(server, args, reply)
			if !ok {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if rf.state != Candidate || rf.currentTerm != termStarted {
				return
			}

			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.state = Follower
				rf.votedFor = -1
				rf.electionResetTime = time.Now()
				return
			}

			if reply.VoteGranted {
				votes++
				if votes >= majority && rf.state == Candidate {
					rf.state = Leader

					for i := range rf.peers {
						rf.nextIndex[i] = len(rf.log)
						rf.matchIndex[i] = 0
					}
					rf.matchIndex[rf.me] = rf.lastLogIndex()
					rf.nextIndex[rf.me] = rf.lastLogIndex() + 1

					go rf.leaderHeartbeatLoop()
				}
			}
		}(i)
	}
}

func (rf *Raft) leaderHeartbeatLoop() {
	for {
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}
		term := rf.currentTerm
		rf.mu.Unlock()

		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			go rf.replicateToPeer(i, term)
		}

		time.Sleep(rf.heartbeatInterval)
	}
}

func (rf *Raft) applier() {
	for {
		rf.mu.Lock()

		if rf.commitIndex <= rf.lastApplied {
			rf.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
			continue
		}

		entries := make([]LogEntry, rf.commitIndex-rf.lastApplied)
		copy(entries, rf.log[rf.lastApplied+1:rf.commitIndex+1])
		startIndex := rf.lastApplied + 1
		rf.lastApplied = rf.commitIndex

		rf.mu.Unlock()

		for i, entry := range entries {
			rf.applyCh <- raftapi.ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: startIndex + i,
			}
		}
	}
}

func (rf *Raft) ticker() {
	for {
		timeout := time.Duration(400+rand.Intn(250)) * time.Millisecond

		rf.mu.Lock()
		state := rf.state
		elapsed := time.Since(rf.electionResetTime)
		rf.mu.Unlock()

		if state != Leader && elapsed >= timeout {
			rf.startElection()
		}

		time.Sleep(10 * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	rf.currentTerm = 0
	rf.votedFor = -1

	// Dummy entry at index 0 so real log starts at index 1.
	rf.log = []LogEntry{{Term: 0, Command: nil}}

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = 1
		rf.matchIndex[i] = 0
	}

	rf.state = Follower
	rf.electionResetTime = time.Now()
	rf.heartbeatInterval = 100 * time.Millisecond

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	// start applier goroutine to deliver committed entries
	go rf.applier()

	return rf
}