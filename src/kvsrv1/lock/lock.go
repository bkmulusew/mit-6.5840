package lock

import (
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck       kvtest.IKVClerk
	lockname string
	id       string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// This interface supports multiple locks by means of the
// lockname argument; locks with different names should be
// independent.
func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
	return &Lock{
		ck:       ck,
		lockname: lockname,
		id:       kvtest.RandValue(8),
	}
}

func (lk *Lock) Acquire() {
	for {
		value, version, err := lk.ck.Get(lk.lockname)

		if err == rpc.OK && value == lk.id {
			return
		}

		if err == rpc.ErrNoKey {
			if lk.ck.Put(lk.lockname, lk.id, 0) == rpc.OK {
				return
			}
			continue
		}

		if err == rpc.OK && value == "" {
			if lk.ck.Put(lk.lockname, lk.id, version) == rpc.OK {
				return
			}
		}
	}
}

func (lk *Lock) Release() {
	for {
		value, version, err := lk.ck.Get(lk.lockname)

		if err == rpc.ErrNoKey || (err == rpc.OK && value != lk.id) {
			return
		}

		if err == rpc.OK && value == lk.id {
			if lk.ck.Put(lk.lockname, "", version) == rpc.OK {
				return
			}
		}
	}
}