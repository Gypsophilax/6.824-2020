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

	leaderId       int // leader id on last time
	clerkId        int // clerk's id
	commandIndex   int
	lastReplyIndex int

	cache map[string]string

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

	ck.leaderId = int(nrand()) % len(ck.servers)
	ck.cache = make(map[string]string)
	ck.clerkId = int(atomic.AddInt64(&clerkIdCount, 1))
	ck.commandIndex = 0
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
	ck.commandIndex++
	clerkId := ck.clerkId
	commandIndex := ck.commandIndex
	count := 1
	for true {
		serverId := ck.leaderId
		ch := make(chan *GetReply)
		go ck.sendGet(key, clerkId, commandIndex, serverId, count, ch)
		var reply *GetReply
		select {
		case reply = <-ch:
			switch reply.Err {
			case ErrWrongLeader, Passed:
				ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			case OK, ErrNoKey:
				ck.lastReplyIndex = max(ck.lastReplyIndex, commandIndex)
				return reply.Value
			}
		case <-time.After(time.Second):
			CPrintf("Clerk %v's %v times %v.Get is timeout", clerkId, count, serverId)
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		}

		count++
		time.Sleep(100 * time.Millisecond)

	}
	return ""
}
func (ck *Clerk) sendGet(key string, clerkId, commandIndex, serverId, count int, ch chan *GetReply) {
	args := &GetArgs{key, clerkId, commandIndex, count}
	reply := &GetReply{Times: count}
	CPrintf("Clerk %v %v times call KVServer %v's Get {args %+v, reply %v}", ck.clerkId, count, serverId, args, reply)
	if ck.servers[serverId].Call("KVServer.Get", args, reply) {
		//  处理 get 的返回结果
		CPrintf("Clerk %v receive KVServer %v's %v times Get reply {args %+v ,reply %v}", ck.clerkId, serverId, count, args, reply)
		//if ck.lastReplyIndex >= commandIndex {
		//	CPrintf("Clerk %v receive a passed Get reply {args %+v, reply %v}", ck.clerkId, args, reply)
		//	return true, Null
		//}

	} else {
		reply.Err = ErrWrongLeader
		CPrintf("Clerk %v lost KVServer %v's %v times Get reply {commandIndex %v}", clerkId, serverId, count, commandIndex)
	}
	ch <- reply
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
	ck.commandIndex++
	clerkId := ck.clerkId
	commandIndex := ck.commandIndex
	count := 1
	for true {
		serverId := ck.leaderId
		ch := make(chan *PutAppendReply)
		go ck.sendPut(key, value, op, clerkId, commandIndex, serverId, count, ch)

		select {
		case reply := <-ch:
			switch reply.Err {
			case ErrWrongLeader, Passed:
				ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			case OK:
				ck.lastReplyIndex = max(ck.lastReplyIndex, commandIndex)
				return
			}

		case <-time.After(time.Second):
			CPrintf("Clerk %v's %v times %v.Get is timeout", clerkId, count, serverId)
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		}
		count++
		time.Sleep(100 * time.Millisecond)
	}
}
func (ck *Clerk) sendPut(key, value, op string, clerkId, commandIndex, serverId, count int, ch chan *PutAppendReply) {
	args := &PutAppendArgs{key, value, op, clerkId, commandIndex, count}
	reply := &PutAppendReply{Times: count}
	CPrintf("Clerk %v %v times call KVServer %v's PutAppend {args %+v, reply %v}", ck.clerkId, count, serverId, args, reply)
	if ck.servers[serverId].Call("KVServer.PutAppend", args, reply) {
		//  处理 put append 的返回结果
		CPrintf("Clerk %v receive KVServer %v's  %v times PutAppend reply {args %+v ,reply %v}", ck.clerkId, serverId, count, args, reply)
		//if ck.lastReplyIndex >= commandIndex {
		//	CPrintf("Clerk %v receive a passed PutAppend reply {args %+v, reply %v}", ck.clerkId, args, reply)
		//	return true, Null
		//}

	} else {
		reply.Err = ErrWrongLeader
		CPrintf("Clerk %v lost KVServer %v's %v times PutAppend reply {commandIndex %v}", clerkId, serverId, count, commandIndex)
	}
	ch <- reply
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
