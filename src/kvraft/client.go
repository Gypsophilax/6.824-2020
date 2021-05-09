package kvraft

import (
	"../labrpc"
	"sync"
	"sync/atomic"
	"time"
)
import "crypto/rand"
import "math/big"

var clerkIdCount int64 = 0

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.

	leaderId     int // leader id on last time
	clerkId      int // clerk's id
	commandCount int

	mu sync.Mutex
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.

	ck.leaderId = Null
	ck.clerkId = int(atomic.AddInt64(&clerkIdCount, 1))
	ck.commandCount = 0
	return ck
}

//
// fetch the current Value for a Key.
// returns "" if the Key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	ck.commandCount++
	args := &GetArgs{key, ck.clerkId, ck.commandCount}
	reply := &GetReply{}
	var serverId int
	result := ""
	if ck.leaderId != Null {
		serverId = ck.leaderId
	} else {
		serverId = int(nrand()) % len(ck.servers)
	}
	for true {
		CPrintf("Clerk %v call KVServer %v's Get {args %v, reply %v}", ck.clerkId, serverId, args, reply)

		if ck.servers[serverId].Call("KVServer.Get", args, reply) {
			// todo 处理 get 的返回结果
			switch reply.Err {
			case ErrWrongLeader:
				if reply.LeaderId == Null || serverId == reply.LeaderId {
					serverId = (serverId + 1) % len(ck.servers)
				} else {
					serverId = reply.LeaderId
				}
			case ErrNoKey:
				break
			case OK:
				result = reply.Value
				ck.leaderId = serverId
				return result
			case ErrNoTypeOpFound:
				CPrintf("Clerk %v: KVServer %v can't deal this type Option 「args %v, reply %v」", ck.clerkId, serverId, args, reply)
				return result
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	return result
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.

	ck.mu.Lock()
	defer ck.mu.Unlock()
	CPrintf("Clerk %v's PutAppend {key %v, value %v, op %v}", ck.clerkId, key, value, op)
	ck.commandCount++
	args := &PutAppendArgs{key, value, op, ck.clerkId, ck.commandCount}
	reply := &PutAppendReply{}
	var serverId int
	if ck.leaderId != Null {
		serverId = ck.leaderId
	} else {
		serverId = int(nrand()) % len(ck.servers)
	}
	for true {
		CPrintf("Clerk %v call KVServer %v's PutAppend {args %v, reply %v}", ck.clerkId, serverId, args, reply)
		if ck.servers[serverId].Call("KVServer.PutAppend", args, reply) {
			// todo 处理 put append 的返回结果
			switch reply.Err {
			case ErrWrongLeader:
				if reply.LeaderId == Null || serverId == reply.LeaderId {
					serverId = (serverId + 1) % len(ck.servers)
				} else {
					serverId = reply.LeaderId
				}
			case OK:
				ck.leaderId = serverId
				return
			case ErrNoTypeOpFound:
				CPrintf("Clerk %v: KVServer %v can't deal this type Option 「args %v, reply %v」", ck.clerkId, serverId, args, reply)
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	CPrintf("Put end")
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
