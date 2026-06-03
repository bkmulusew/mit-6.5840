package kvraft

import (
	"sync"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

type KVServer struct {
	mu sync.Mutex
	me  int
	rsm *rsm.RSM
	kv map[string]string
	version map[string]rpc.Tversion
}

// To type-cast req to the right type, take a look at Go's type switches or type
// assertions below:
//
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15
func (kv *KVServer) DoOp(req any) any {
	// Your code here
	switch args := req.(type) {
	case rpc.GetArgs:
		kv.mu.Lock()
		defer kv.mu.Unlock()
		reply := rpc.GetReply{}
		value, ok := kv.kv[args.Key]
		if !ok {
			reply.Err = rpc.ErrNoKey
			return reply
		}

		reply.Value = value
		reply.Version = kv.version[args.Key]
		reply.Err = rpc.OK
		return reply
	case rpc.PutArgs:
		kv.mu.Lock()
		defer kv.mu.Unlock()

		reply := rpc.PutReply{}
		version, ok := kv.version[args.Key]

		if !ok {
			if args.Version == 0 {
				kv.kv[args.Key] = args.Value
				kv.version[args.Key] = 1
				reply.Err = rpc.OK
			} else {
				reply.Err = rpc.ErrNoKey
			}
			return reply
		}

		if version != args.Version {
			reply.Err = rpc.ErrVersion
			return reply
		}

		kv.kv[args.Key] = args.Value
		kv.version[args.Key] = version + 1
		reply.Err = rpc.OK
		return reply
	}
	return nil
}

func (kv *KVServer) Snapshot() []byte {
	// Your code here
	return nil
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	err, rep := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	result := rep.(rpc.GetReply)
	reply.Value = result.Value
	reply.Version = result.Version
	reply.Err = result.Err
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	err, rep := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	reply.Err = rep.(rpc.PutReply).Err
}

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []any {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})

	kv := &KVServer{
		me:      me,
		kv:      make(map[string]string),
		version: make(map[string]rpc.Tversion),
	}

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	return []any{kv, kv.rsm.Raft()}
}

func NewServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, grp tester.Tgid, srv int, persister *tester.Persister) []any {
	return StartKVServer(ends, Gid, srv, persister, tester.MaxRaftState)
}
