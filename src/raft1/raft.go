package raft

// The file ../raftapi/raftapi.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// In addition,  Make() creates a new raft peer that implements the
// raft interface.

import (
	"bytes"
	"math/rand"
	"sync"
	"time"

	"6.5840/labgob"
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

const snapshotChunkSize = 256 * 1024 // 256 KB per InstallSnapshot chunk

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Persistent state on all servers
	currentTerm int
	votedFor    int
	// log[0] is a dummy entry whose Term equals lastIncludedTerm.
	// log[i] for i >= 1 holds the entry whose real index is lastIncludedIndex + i.
	log               []LogEntry
	lastIncludedIndex int
	lastIncludedTerm  int

	// Latest snapshot bytes (kept so we can resave on persist()).
	snapshot []byte

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

	// Follower-side buffer for reassembling a chunked InstallSnapshot transfer.
	snapBuf      []byte
	snapBufIndex int // LastIncludedIndex of the snapshot being assembled
	snapBufTerm  int // LastIncludedTerm of the snapshot being assembled
}

// return currentTerm and whether this server believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}

func (rf *Raft) encodeState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	return w.Bytes()
}

// save Raft's persistent state (and the current snapshot) to stable storage.
func (rf *Raft) persist() {
	rf.persister.Save(rf.encodeState(), rf.snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	var lastIncludedIndex int
	var lastIncludedTerm int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
		return
	}
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.log = log
	rf.lastIncludedIndex = lastIncludedIndex
	rf.lastIncludedTerm = lastIncludedTerm
	rf.commitIndex = lastIncludedIndex
	rf.lastApplied = lastIncludedIndex
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index <= rf.lastIncludedIndex {
		return
	}
	if index > rf.lastApplied {
		return
	}

	pos := index - rf.lastIncludedIndex
	term := rf.log[pos].Term

	newLog := make([]LogEntry, 0, len(rf.log) - pos)
	newLog = append(newLog, LogEntry{Term: term, Command: nil})
	newLog = append(newLog, rf.log[pos + 1:]...)
	rf.log = newLog
	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = term
	rf.snapshot = snapshot

	rf.persist()
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

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Offset            int
	Data              []byte
	Done              bool
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) lastLogIndex() int {
	return rf.lastIncludedIndex + len(rf.log) - 1
}

func (rf *Raft) lastLogTerm() int {
	return rf.log[len(rf.log) - 1].Term
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
		rf.persist()
	}

	reply.Term = rf.currentTerm

	myLastIndex, myLastTerm := rf.lastLogIndexAndTerm()
	upToDate := args.LastLogTerm > myLastTerm ||
		(args.LastLogTerm == myLastTerm && args.LastLogIndex >= myLastIndex)

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && upToDate {
		rf.votedFor = args.CandidateId
		rf.electionResetTime = time.Now()
		reply.VoteGranted = true
		rf.persist()
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
		rf.persist()
	}
	rf.state = Follower
	rf.electionResetTime = time.Now()
	reply.Term = rf.currentTerm

	prevLogIndex := args.PrevLogIndex
	prevLogTerm := args.PrevLogTerm
	entries := args.Entries

	if prevLogIndex < rf.lastIncludedIndex {
		// Some prefix of these entries is already covered by our snapshot.
		skip := rf.lastIncludedIndex - prevLogIndex
		if skip > len(entries) {
			reply.Success = true
			if args.LeaderCommit > rf.commitIndex {
				rf.commitIndex = min(args.LeaderCommit, rf.lastLogIndex())
			}
			return
		}
		prevLogIndex = rf.lastIncludedIndex
		prevLogTerm = rf.lastIncludedTerm
		entries = entries[skip:]
	}

	prevPos := prevLogIndex - rf.lastIncludedIndex

	// 1. Follower is missing PrevLogIndex.
	if prevPos >= len(rf.log) {
		reply.ConflictIndex = rf.lastIncludedIndex + len(rf.log)
		return
	}

	// 2. PrevLogTerm mismatch.
	if rf.log[prevPos].Term != prevLogTerm {
		reply.ConflictTerm = rf.log[prevPos].Term

		conflictPos := prevPos
		for conflictPos > 1 && rf.log[conflictPos - 1].Term == reply.ConflictTerm {
			conflictPos--
		}
		reply.ConflictIndex = conflictPos + rf.lastIncludedIndex
		return
	}

	// 3. Append new entries, deleting conflicting suffix first.
	insertPos := prevPos + 1
	i := 0

	for i < len(entries) {
		if insertPos + i >= len(rf.log) {
			break
		}
		if rf.log[insertPos + i].Term != entries[i].Term {
			rf.log = rf.log[:insertPos + i]
			break
		}
		i++
	}

	if i < len(entries) {
		rf.log = append(rf.log, entries[i:]...)
	}

	rf.persist()

	// 4. Update commit index from leader.
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.lastLogIndex())
	}

	reply.Success = true
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

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

	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		return
	}

	// Chunk reassembly: Offset==0 starts a fresh transfer; any other offset
	// must exactly match the current buffer length (in-order delivery assumed).
	if args.Offset == 0 {
		rf.snapBuf = make([]byte, 0, len(args.Data))
		rf.snapBufIndex = args.LastIncludedIndex
		rf.snapBufTerm = args.LastIncludedTerm
	} else if args.LastIncludedIndex != rf.snapBufIndex ||
		args.LastIncludedTerm != rf.snapBufTerm ||
		args.Offset != len(rf.snapBuf) {
		// Stale or out-of-order chunk — discard.
		return
	}

	rf.snapBuf = append(rf.snapBuf, args.Data...)

	if !args.Done {
		return
	}

	// Full snapshot assembled — apply it.
	data := rf.snapBuf
	rf.snapBuf = nil

	pos := args.LastIncludedIndex - rf.lastIncludedIndex
	if pos < len(rf.log) && rf.log[pos].Term == args.LastIncludedTerm {
		newLog := make([]LogEntry, 0, len(rf.log) - pos)
		newLog = append(newLog, LogEntry{Term: args.LastIncludedTerm, Command: nil})
		newLog = append(newLog, rf.log[pos + 1:]...)
		rf.log = newLog
	} else {
		rf.log = []LogEntry{{Term: args.LastIncludedTerm, Command: nil}}
	}

	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.snapshot = data

	if rf.commitIndex < args.LastIncludedIndex {
		rf.commitIndex = args.LastIncludedIndex
	}

	rf.persist()
}

// example code to send a RequestVote RPC to a server.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.peers[server].Call("Raft.AppendEntries", args, reply)
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	return rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
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
	rf.persist()
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

// sendSnapshotToPeer sends the current snapshot to server in snapshotChunkSize
// chunks. It sends chunks sequentially, re-checking leadership between each
// one. Only updates matchIndex/nextIndex after the final chunk is acknowledged.
func (rf *Raft) sendSnapshotToPeer(server int, term int) {
	rf.mu.Lock()
	if rf.state != Leader || rf.currentTerm != term {
		rf.mu.Unlock()
		return
	}
	snapData := rf.snapshot
	snapIndex := rf.lastIncludedIndex
	snapTerm := rf.lastIncludedTerm
	rf.mu.Unlock()

	offset := 0
	for {
		end := offset + snapshotChunkSize
		if end > len(snapData) {
			end = len(snapData)
		}
		done := end == len(snapData)

		args := &InstallSnapshotArgs{
			Term:              term,
			LeaderId:          rf.me,
			LastIncludedIndex: snapIndex,
			LastIncludedTerm:  snapTerm,
			Offset:            offset,
			Data:              snapData[offset:end],
			Done:              done,
		}
		reply := &InstallSnapshotReply{}
		if ok := rf.sendInstallSnapshot(server, args, reply); !ok {
			return
		}

		rf.mu.Lock()
		if rf.state != Leader || rf.currentTerm != term {
			rf.mu.Unlock()
			return
		}
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = Follower
			rf.votedFor = -1
			rf.persist()
			rf.electionResetTime = time.Now()
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()

		if done {
			break
		}
		offset = end
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader || rf.currentTerm != term {
		return
	}
	if snapIndex > rf.matchIndex[server] {
		rf.matchIndex[server] = snapIndex
	}
	if snapIndex+1 > rf.nextIndex[server] {
		rf.nextIndex[server] = snapIndex + 1
	}
	rf.updateCommitIndex()
}

func (rf *Raft) replicateToPeer(server int, term int) {
	rf.mu.Lock()

	if rf.state != Leader || rf.currentTerm != term {
		rf.mu.Unlock()
		return
	}

	nextIdx := rf.nextIndex[server]

	if nextIdx <= rf.lastIncludedIndex {
		rf.mu.Unlock()
		rf.sendSnapshotToPeer(server, term)
		return
	}

	prevLogIndex := nextIdx - 1
	prevLogTerm := rf.log[prevLogIndex - rf.lastIncludedIndex].Term

	sliceStart := nextIdx - rf.lastIncludedIndex
	entries := make([]LogEntry, len(rf.log) - sliceStart)
	copy(entries, rf.log[sliceStart:])

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
		rf.persist()
		rf.electionResetTime = time.Now()
		return
	}

	if reply.Success {
		match := args.PrevLogIndex + len(args.Entries)
		if match > rf.matchIndex[server] {
			rf.matchIndex[server] = match
		}
		if match + 1 > rf.nextIndex[server] {
			rf.nextIndex[server] = match + 1
		}
		rf.updateCommitIndex()
		return
	}

	// Backtrack nextIndex using follower conflict info.
	if reply.ConflictTerm != -1 {
		lastIndexOfTerm := -1
		for i := len(rf.log) - 1; i >= 1; i-- {
			if rf.log[i].Term == reply.ConflictTerm {
				lastIndexOfTerm = i + rf.lastIncludedIndex
				break
			}
		}
		if lastIndexOfTerm == -1 && rf.lastIncludedTerm == reply.ConflictTerm {
			lastIndexOfTerm = rf.lastIncludedIndex
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
	for n := rf.lastLogIndex(); n > rf.commitIndex && n > rf.lastIncludedIndex; n-- {
		if rf.log[n - rf.lastIncludedIndex].Term != rf.currentTerm {
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
	rf.persist()
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
				rf.persist()
				rf.electionResetTime = time.Now()
				return
			}

			if reply.VoteGranted {
				votes++
				if votes >= majority && rf.state == Candidate {
					rf.state = Leader

					last := rf.lastLogIndex()
					for i := range rf.peers {
						rf.nextIndex[i] = last + 1
						rf.matchIndex[i] = 0
					}
					rf.matchIndex[rf.me] = last
					rf.nextIndex[rf.me] = last + 1

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

		// A snapshot is owed to the service whenever the snapshot
		// boundary moved past what we've already delivered.
		if rf.lastIncludedIndex > rf.lastApplied {
			snap := rf.snapshot
			idx := rf.lastIncludedIndex
			term := rf.lastIncludedTerm
			rf.lastApplied = idx
			rf.mu.Unlock()

			rf.applyCh <- raftapi.ApplyMsg{
				SnapshotValid: true,
				Snapshot:      snap,
				SnapshotIndex: idx,
				SnapshotTerm:  term,
			}
			continue
		}

		if rf.commitIndex <= rf.lastApplied {
			rf.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
			continue
		}

		startIndex := rf.lastApplied + 1
		endIndex := rf.commitIndex

		sliceStart := startIndex - rf.lastIncludedIndex
		sliceEnd := endIndex - rf.lastIncludedIndex

		entries := make([]LogEntry, sliceEnd - sliceStart + 1)
		copy(entries, rf.log[sliceStart : sliceEnd + 1])
		rf.lastApplied = endIndex

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
		timeout := time.Duration(400 + rand.Intn(250)) * time.Millisecond

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

	// Dummy entry at slice index 0 representing the snapshot boundary.
	rf.log = []LogEntry{{Term: 0, Command: nil}}
	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0

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

	rf.snapshot = persister.ReadSnapshot()
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	// start applier goroutine to deliver committed entries
	go rf.applier()

	return rf
}
