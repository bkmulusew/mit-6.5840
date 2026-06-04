package rsm

import (
	"sync"
	"sync/atomic"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	raft "6.5840/raft1"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type Op struct {
	Me  int
	Id  int64
	Req any
}

// A waiter is a Submit() call blocked at a particular Raft log index,
// waiting for the reader goroutine to tell it how its op turned out.
type waiter struct {
	id int64    // op id this Submit is waiting for at its index
	ch chan any // reader sends DoOp's result; closes it if leadership was lost
}

// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine
	nextId       int64
	pending      map[int]*waiter
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
		pending:      make(map[int]*waiter),
	}
	if !tester.UseRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}
	snapshot := persister.ReadSnapshot()
	if len(snapshot) > 0 {
		rsm.sm.Restore(snapshot)
	}
	go rsm.reader()
	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {
	id := atomic.AddInt64(&rsm.nextId, 1)
	index, term, isLeader := rsm.rf.Start(Op{Me: rsm.me, Id: id, Req: req})
	if !isLeader {
		return rpc.ErrWrongLeader, nil
	}

	// Register a waiter at our log index, then block until the reader
	// resolves it.
	ch := make(chan any, 1)
	rsm.mu.Lock()
	rsm.pending[index] = &waiter{id: id, ch: ch}
	rsm.mu.Unlock()
	defer func() {
		rsm.mu.Lock()
		delete(rsm.pending, index)
		rsm.mu.Unlock()
	}()

	for {
		select {
		case rep, ok := <-ch:
			if !ok {
				// Channel closed: a different op committed at our index,
				// so our op was lost.
				return rpc.ErrWrongLeader, nil
			}
			return rpc.OK, rep
		case <-time.After(20 * time.Millisecond):
			// If we stopped being the leader for our term, our op will
			// never commit; tell the client to retry elsewhere.
			if t, leader := rsm.rf.GetState(); !leader || t != term {
				return rpc.ErrWrongLeader, nil
			}
		}
	}
}

// reader is the sole consumer of applyCh. It applies every committed
// entry to the state machine in log order, then wakes the matching
// Submit (if any) on this server. Snapshots are taken inline so the
// index passed to rf.Snapshot always matches the applied state exactly.
func (rsm *RSM) reader() {
	for msg := range rsm.applyCh {
		switch {
		case msg.SnapshotValid:
			rsm.sm.Restore(msg.Snapshot)
		case msg.CommandValid:
			op := msg.Command.(Op)
			rep := rsm.sm.DoOp(op.Req)
			rsm.wake(msg.CommandIndex, op, rep)
			if rsm.maxraftstate != -1 && rsm.rf.PersistBytes() >= rsm.maxraftstate {
				if snapshot := rsm.sm.Snapshot(); snapshot != nil {
					rsm.rf.Snapshot(msg.CommandIndex, snapshot)
				}
			}
		}
	}
	rsm.wakeAll() // applyCh closed on shutdown
}

// wake resolves the Submit (if any) waiting at index.
func (rsm *RSM) wake(index int, op Op, rep any) {
	rsm.mu.Lock()
	defer rsm.mu.Unlock()
	w, ok := rsm.pending[index]
	if !ok {
		return
	}
	if op.Me == rsm.me && op.Id == w.id {
		w.ch <- rep // our op committed: deliver the result
	} else {
		close(w.ch) // someone else's op landed here: wrong leader
	}
}

// wakeAll fails every outstanding Submit, used when applyCh closes.
func (rsm *RSM) wakeAll() {
	rsm.mu.Lock()
	defer rsm.mu.Unlock()
	for _, w := range rsm.pending {
		close(w.ch)
	}
	rsm.pending = make(map[int]*waiter)
}
