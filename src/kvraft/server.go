package kvraft

import (
	"../labgob"
	"../labrpc"
	"../raft"
	"container/list"
	"log"
	"sync"
	"sync/atomic"
)

const Debug = 1

func DPrintf(server *KVServer, format string, a ...interface{}) (n int, err error) {
	if Debug > 0 && !server.killed() {
		log.Printf(format, a...)
	}
	return
}

func CPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Leader struct {
	rfIndexOpMap *sync.Map   // raft log index -> rpc handler (Option)
	clerkIndex   map[int]int // clerk's id -> clerk's last command index
	ops          *list.List  // all rpc handlers that wait leader to notify
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kv       *sync.Map // store Key/Value
	term     int       // raft term
	isLeader bool
	leader   *Leader

	leaderId int
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	// todo 获取
	op := kv.getOp(args)
	kv.mu.Lock()
	//
	command := *op.command
	index, term, isLeader := kv.rf.Start(command)
	op.raftIndex = index
	if isLeader {
		kv.becomeLeader(term)
		// todo check clerk command index
		if kv.checkDuplicate(op, args.CommandIndex) {
			reply.Err = Passed
			reply.LeaderId = kv.leaderId
			kv.mu.Unlock()
			return
		}
		ops := []*Option{op}
		if v, ok := kv.leader.rfIndexOpMap.Load(op.raftIndex); ok {
			ops = append(ops, v.([]*Option)...)
		}
		kv.leader.rfIndexOpMap.Store(op.raftIndex, ops)
		op.element = kv.leader.ops.PushBack(op)
		kv.mu.Unlock()
		// 等待唤醒
		<-op.notify
		kv.doGetNotify(op, reply)
	} else {
		kv.becomeFollower(term)
		reply.Err = ErrWrongLeader
		reply.LeaderId = kv.leaderId
		kv.mu.Unlock()
	}
}

func (kv *KVServer) doGetNotify(op *Option, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// 是唤醒之后进行删除？
	//if v, ok := kv.doingOp.Load(Option.commandId); ok {
	//	ops := v.([]*Option)
	//	for i := range ops {
	//		if ops[i].Id == Option.Id {
	//			ops = append(ops[:i], ops[i+1:]...)
	//			break
	//		}
	//	}
	//}
	reply.Err = op.err
	reply.Value = op.command.Value
	reply.LeaderId = kv.leaderId

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// todo 添加和修改
	//
	DPrintf(kv, "KVServer %v receive Clerk %v's PutAppend {args %v, reply %v}", kv.me, args.ClerkId, args, reply)
	op := kv.putAppendOp(args)

	kv.mu.Lock()
	command := *op.command
	index, term, isLeader := kv.rf.Start(command)
	op.raftIndex = index
	if isLeader {
		kv.becomeLeader(term)
		// todo check clerk command index
		if kv.checkDuplicate(op, args.CommandIndex) {
			reply.Err = Passed
			reply.LeaderId = kv.leaderId
			kv.mu.Unlock()
			return
		}
		ops := []*Option{op}
		if v, ok := kv.leader.rfIndexOpMap.Load(op.raftIndex); ok {
			ops = append(ops, v.([]*Option)...)
		}
		kv.leader.rfIndexOpMap.Store(op.raftIndex, ops)
		op.element = kv.leader.ops.PushBack(op)
		kv.mu.Unlock()
		// 等待唤醒
		<-op.notify
		kv.doPutAppendNotify(op, reply)
	} else {
		kv.becomeFollower(term)
		reply.Err = ErrWrongLeader
		reply.LeaderId = kv.leaderId
		kv.mu.Unlock()
	}
}

func (kv *KVServer) doPutAppendNotify(op *Option, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Err = op.err
	reply.LeaderId = kv.leaderId
}

func (kv *KVServer) checkDuplicate(op *Option, commandIndex int) bool {
	clerkIndex, ok := kv.leader.clerkIndex[op.clerkId]
	if ok && clerkIndex >= commandIndex {
		// this command is passed
		return true
	} else {
		// update clerk's command index
		kv.leader.clerkIndex[op.clerkId] = op.clerkIndex

		return false
	}
}

// 向 Raft 发送 Option

func (kv *KVServer) applyLoop() {
	for !kv.killed() {

		if applyMsg := <-kv.applyCh; applyMsg.CommandValid {
			command := applyMsg.Command.(Op)
			// 检测是否还是Leader
			DPrintf(kv, "KVServer %v receive command %v", kv.me, command)
			kv.mu.Lock()
			// 检测是否是 Leader
			if kv.isLeader {
				if kv.term != applyMsg.Term { // 检测 leader
					kv.becomeFollower(applyMsg.Term)
					kv.doWork(&command)
					kv.mu.Unlock()
					continue
				}

				err, value := kv.doWork(&command)

				// notify rpc handler that blocked on this raft index
				kv.notifyOps(applyMsg.CommandIndex, err, value, &command)
				kv.mu.Unlock()
			} else {
				kv.doWork(&command)
				kv.mu.Unlock()
			}
		}

	}

}

func (kv *KVServer) notifyOps(rfIndex int, err Err, value string, command *Op) {
	if v, ok := kv.leader.rfIndexOpMap.Load(rfIndex); ok {
		ops := v.([]*Option)
		for _, op := range ops {
			op.err = err
			if op.raftIndex == rfIndex && op.sameCommand(command) {
				op.command.Value = value
			} else {
				op.err = ErrWrongLeader
			}
			op.notify <- op
			kv.leader.ops.Remove(op.element)
		}
		// clear poke
		kv.leader.rfIndexOpMap.Delete(rfIndex)
	}
}

func (kv *KVServer) detectLeaderLoop() {

}

func (kv *KVServer) becomeFollower(term int) {
	kv.term = term
	kv.isLeader = false
	ops := kv.leader.ops
	// clear leader state
	kv.leader.ops = list.New()
	kv.leader.clerkIndex = make(map[int]int)
	kv.leader.rfIndexOpMap = &sync.Map{}
	// todo 对待处理的请求返回结果
	go kv.rejectOps(ops)
}

func (kv *KVServer) becomeLeader(term int) {
	kv.term = term
	kv.isLeader = true
	kv.leaderId = kv.me
}

func (kv *KVServer) rejectOps(ops *list.List) {
	for op := ops.Front(); op != nil; op = op.Next() {
		op := op.Value.(*Option)
		op.err = ErrWrongLeader
		op.notify <- op
	}
}
func (kv *KVServer) doWork(command *Op) (Err, string) {
	switch command.Op {
	case PutOp:
		return kv.doPut(command)
	case GetOp:
		return kv.doGet(command)
	case AppendOp:
		return kv.doAppend(command)
	}
	return ErrNoTypeOpFound, ""
}

func (kv *KVServer) doPut(command *Op) (Err, string) {
	kv.kv.Store(command.Key, command.Value)
	return OK, command.Value
}

func (kv *KVServer) doAppend(command *Op) (Err, string) {
	v, ok := kv.kv.Load(command.Key)
	var value string
	if ok {
		value = v.(string) + command.Value
	} else {
		value = command.Value
	}
	kv.kv.Store(command.Key, value)
	return OK, value
}

func (kv *KVServer) doGet(command *Op) (Err, string) {
	v, ok := kv.kv.Load(command.Key)
	if ok {
		command.Value = v.(string)
		return OK, command.Value
	} else {
		return ErrNoKey, ""
	}

}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant Key/Value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.kv = &sync.Map{}
	kv.term = 0
	kv.isLeader = false
	kv.leader = &Leader{&sync.Map{}, make(map[int]int), list.New()}
	kv.leaderId = -1
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	go kv.applyLoop()
	// You may need initialization code here.

	return kv
}
