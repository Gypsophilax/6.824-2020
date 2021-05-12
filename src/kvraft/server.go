package kvraft

import (
	"../labgob"
	"../labrpc"
	"../raft"
	"bytes"
	"container/list"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = 1

func DPrintf(server *KVServer, format string, a ...interface{}) (n int, err error) {
	if Debug > 0 && !server.killed() {
		fmt.Printf("        "+format+"\n", a...)
	}
	return
}

func CPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		fmt.Printf("    "+format+"\n", a...)
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
	kv       map[string]string // store Key/Value
	term     int               // raft term
	isLeader bool
	leader   *Leader

	leaderId                int
	lastAppliedCommandIndex int
	lastAppliedCommandTerm  int
	clerkCache              map[int]map[int]string // cache result of already success command
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	//  获取
	reply.Times = args.Times
	kv.mu.Lock()
	term, isLeader := kv.rf.GetState()
	index := -1
	DPrintf(kv, "KVServer %v handle Get {args %+v, reply %v}", kv.me, args, reply)
	defer DPrintf(kv, "KVServer %v return Get {args %+v, reply %v}", kv.me, args, reply)
	if isLeader {
		op := kv.getOp(args)
		command := op.command
		reply.LeaderId = kv.leaderId

		//if lastIndex, ok := kv.leader.clerkIndex[command.ClerkId]; ok && command.ClerkIndex > lastIndex {
		//	if cache, ok := kv.clerkCache[command.ClerkId]; ok {
		//		for k, _ := range cache {
		//			if k <= lastIndex {
		//				delete(cache, k)
		//			}
		//		}
		//	}
		//}
		// 查看缓存
		//if cache, ok := kv.clerkCache[command.ClerkId]; ok {
		//	if value, ok := cache[command.ClerkIndex]; ok {
		//		reply.Err = OK
		//		reply.Value = value
		//
		//		kv.mu.Unlock()
		//		return
		//	}
		//}
		//if kv.isDuplicated(op.command) {
		//	reply.Err = Passed
		//	kv.mu.Unlock()
		//	return
		//}

		index, term, isLeader = kv.rf.Start(*op.command)
		DPrintf(kv, "KVServer %v handle Clerk %v's Get {args %+v, reply %v， option %v, op %v}\n Start return {index %v, term %v ,isLeader %v}", kv.me, args.ClerkId, args, reply, op, op.command, index, term, isLeader)

		if isLeader {

			op.raftIndex = index
			kv.becomeLeader(term)
			reply.LeaderId = kv.leaderId
			//  check clerk command index
			//ops := []*Option{op}
			//if v, ok := kv.leader.rfIndexOpMap.Load(op.raftIndex); ok {
			//	ops = append(ops, v.([]*Option)...)
			//}
			kv.leader.rfIndexOpMap.Store(op.raftIndex, op)
			op.element = kv.leader.ops.PushBack(op)
			DPrintf(kv, "command %v is wait on index %v", command, index)
			kv.mu.Unlock()
			// 等待唤醒
			select {
			case nop := <-op.notify:
				kv.mu.Lock()
				DPrintf(kv, "KVServer %v: {ClerkId %v ,ClerkIndex %v, op %v} is notified {reply %v}", kv.me, op.command.ClerkId, op.command.ClerkIndex, op.command, reply)

				if !kv.sameCommand(nop, op) {
					op.err = ErrWrongLeader
				} else {
					kv.doGetNotify(op, reply)
				}
				kv.mu.Unlock()
			case <-time.After(time.Second):
				kv.mu.Lock()
				op.err = ErrWrongLeader
				DPrintf(kv, "command %v, index %v is timeout", command, index)
				kv.leader.ops.Remove(op.element)
				// clear poke
				kv.leader.rfIndexOpMap.Delete(index)
				kv.mu.Unlock()
			}
			return
		}
	}
	kv.becomeFollower(term)
	reply.Err = ErrWrongLeader
	reply.LeaderId = kv.leaderId
	kv.mu.Unlock()
}

func (kv *KVServer) doGetNotify(op *Option, reply *GetReply) {

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
	//  添加和修改
	//
	reply.Times = args.Times
	kv.mu.Lock()
	term, isLeader := kv.rf.GetState()
	index := -1
	DPrintf(kv, "KVServer %v handle PutAppend {args %+v, reply %v}", kv.me, args, reply)
	defer DPrintf(kv, "KVServer %v return PutAppend {args %+v, reply %v}", kv.me, args, reply)
	if isLeader {
		op := kv.putAppendOp(args)

		command := op.command

		//if lastIndex, ok := kv.leader.clerkIndex[command.ClerkId]; ok && command.ClerkIndex > lastIndex {
		//	if cache, ok := kv.clerkCache[command.ClerkId]; ok {
		//		for k, _ := range cache {
		//			if k <= lastIndex {
		//				delete(cache, k)
		//			}
		//		}
		//	}
		//}
		//// 查看缓存
		//if cache, ok := kv.clerkCache[command.ClerkId]; ok {
		//	if _, ok := cache[command.ClerkIndex]; ok {
		//		reply.Err = OK
		//		reply.LeaderId = kv.leaderId
		//		kv.mu.Unlock()
		//		return
		//	}
		//}
		////  check clerk command index
		//if kv.isDuplicated(op.command) {
		//	reply.Err = Passed
		//	reply.LeaderId = kv.leaderId
		//	kv.mu.Unlock()
		//	return
		//}
		index, term, isLeader = kv.rf.Start(*op.command)
		DPrintf(kv, "KVServer %v handle Clerk %v's PutAppend {args %+v, reply %v， option %v, op %v}\n Start return {index %v, term %v ,isLeader %v}", kv.me, args.ClerkId, args, reply, op, op.command, index, term, isLeader)

		if isLeader {
			op.raftIndex = index
			kv.becomeLeader(term)
			//ops := []*Option{op}
			//if v, ok := kv.leader.rfIndexOpMap.Load(op.raftIndex); ok {
			//	ops = append(ops, v.([]*Option)...)
			//}
			kv.leader.rfIndexOpMap.Store(op.raftIndex, op)
			op.element = kv.leader.ops.PushBack(op)
			DPrintf(kv, "command %v is wait on index %v", command, index)
			kv.mu.Unlock()
			// 等待唤醒

			select {
			case nop := <-op.notify:
				kv.mu.Lock()
				DPrintf(kv, "KVServer %v: {ClerkId %v ,ClerkIndex %v, op %v} is notified {reply %v}", kv.me, op.command.ClerkId, op.command.ClerkIndex, op.command, reply)
				if !kv.sameCommand(nop, op) {
					op.err = ErrWrongLeader
				} else {

					kv.doPutAppendNotify(op, reply)
				}
				kv.mu.Unlock()
			case <-time.After(time.Second):
				kv.mu.Lock()
				op.err = ErrWrongLeader
				DPrintf(kv, "command %v, index %v is timeout", command, index)
				kv.leader.ops.Remove(op.element)
				// clear poke
				kv.leader.rfIndexOpMap.Delete(index)
				kv.mu.Unlock()
			}
			return
		}
	}
	kv.becomeFollower(term)
	reply.Err = ErrWrongLeader
	reply.LeaderId = kv.leaderId
	kv.mu.Unlock()
}

func (kv *KVServer) doPutAppendNotify(op *Option, reply *PutAppendReply) {

	reply.Err = op.err
	reply.LeaderId = kv.leaderId

}

func (kv *KVServer) isDuplicated(command *Command) bool {
	clerkIndex, ok := kv.leader.clerkIndex[command.ClerkId]

	if ok && clerkIndex >= command.ClerkIndex {
		// this command is passed
		value, _ := kv.kv[command.Key]
		DPrintf(kv, "KVServer %v : %v is duplicated {last index %v, value %v}", kv.me, command, clerkIndex, value)
		return true
	} else {
		// update clerk's command index
		//kv.leader.ClerkIndex[command.ClerkId] = command.ClerkIndex
		value, _ := kv.kv[command.Key]
		DPrintf(kv, "KVServer %v : %v is passed {last index %v, value %v}", kv.me, command, clerkIndex, value)

		return false
	}
}

// 向 Raft 发送 Option

func (kv *KVServer) applyLoop() {
	for !kv.killed() {
		applyMsg := <-kv.applyCh
		kv.mu.Lock()
		DPrintf(kv, "KVServer %v{term %v, isLeader %v, leaderId %v, lastCommandIndex %v, lastCommandTerm %V} receive from raft {op %v, applyMsg %v}", kv.me, kv.term, kv.isLeader, kv.leaderId, kv.lastAppliedCommandIndex, kv.lastAppliedCommandTerm, applyMsg.Command, applyMsg)

		if applyMsg.CommandValid {
			command := applyMsg.Command.(Command)

			// 检测是否是 Leader
			if kv.isLeader {
				if kv.term != applyMsg.Term { // 检测 leader
					kv.becomeFollower(applyMsg.Term)
					kv.doWork(&command, applyMsg.CommandIndex, applyMsg.Term)
					kv.mu.Unlock()
					continue
				}

				err, value := kv.doWork(&command, applyMsg.CommandIndex, applyMsg.Term)

				// notify rpc handler that blocked on this raft index
				kv.notifyOps(applyMsg.CommandIndex, err, value, &command)
				kv.mu.Unlock()
			} else {
				kv.doWork(&command, applyMsg.CommandIndex, applyMsg.Term)
				kv.mu.Unlock()
			}
		} else {
			// 接收到了快照
			if kv.lastAppliedCommandIndex < applyMsg.SnapshotIndex || kv.lastAppliedCommandTerm < applyMsg.SnapshotTerm {
				ok := kv.rf.CondInstallSnapshot(applyMsg.SnapshotTerm, applyMsg.SnapshotIndex, applyMsg.Snapshot)
				if ok {
					// todo 改变KVServer的状态
					DPrintf(kv, "KVServer %v change state", kv.me)
					kv.loadSnapshot(applyMsg.Snapshot)
				}
			} else {
				DPrintf(kv, "KVServer %v' s state is newer", kv.me)
			}
			kv.mu.Unlock()
		}

	}

}

func (kv *KVServer) notifyOps(rfIndex int, err Err, value string, command *Command) {

	if v, ok := kv.leader.rfIndexOpMap.Load(rfIndex); ok {
		//ops := v.([]*Option)
		//for _, op := range ops {
		//	op.err = err
		//	if op.raftIndex == rfIndex && op.sameCommand(command) {
		//		op.command.Value = value
		//	} else {
		//		op.err = ErrWrongLeader
		//	}
		//	DPrintf(kv, "KVServer %v notify {option %v ,op %v}", kv.me, op, op.command)
		//	op.notify <- op
		//	kv.leader.ops.Remove(op.element)
		//}
		op := v.(*Option)
		op.err = err
		//if op.raftIndex == rfIndex && op.sameCommand(command) {
		//	op.command.Value = value
		//} else {
		//	op.err = ErrWrongLeader
		//}
		op.command.Value = value
		DPrintf(kv, "KVServer %v notify {option %v ,op %v}", kv.me, op, op.command)
		op.notify <- op
		kv.leader.ops.Remove(op.element)
		// clear poke
		kv.leader.rfIndexOpMap.Delete(rfIndex)
		DPrintf(kv, "KVServer %v notify ops that is waiting on raftIndex %v {op %v}", kv.me, rfIndex, command)
	} else {
		DPrintf(kv, "command %v, index %v is timeout", command, rfIndex)
	}
}

func (kv *KVServer) becomeFollower(term int) {
	kv.term = term
	kv.isLeader = false
	ops := kv.leader.ops
	// clear leader State
	kv.leader.ops = list.New()
	//  对待处理的请求返回结果
	//DPrintf(kv, "KVServer %v do follower {term %v, isLeader %v, leaderId %v}", kv.me, kv.term, kv.isLeader, kv.leaderId)

	go kv.rejectOps(ops)
}

func (kv *KVServer) becomeLeader(term int) {
	kv.term = term
	kv.isLeader = true
	kv.leaderId = kv.me
	DPrintf(kv, "KVServer %v do leader {term %v, isLeader %v, leaderId %v, ops size %v}", kv.me, kv.term, kv.isLeader, kv.leaderId, kv.leader.ops.Len())
}

func (kv *KVServer) rejectOps(ops *list.List) {
	for op := ops.Front(); op != nil; op = op.Next() {
		op := op.Value.(*Option)
		op.err = ErrWrongLeader
		DPrintf(kv, "KVServer %v reject {option %v, op %v}", kv.me, op, op.command)
		op.notify <- op
	}
}
func (kv *KVServer) doWork(command *Command, commandIndex, commandTerm int) (Err, string) {

	if kv.isDuplicated(command) || kv.lastAppliedCommandIndex >= commandIndex {
		////  clear cache
		//if lastIndex, ok := kv.leader.clerkIndex[command.ClerkId]; ok {
		//	if cache, ok := kv.clerkCache[command.ClerkId]; ok {
		//		for k, _ := range cache {
		//			if k < lastIndex {
		//				delete(cache, k)
		//			}
		//		}
		//	}
		//}
		DPrintf(kv, "%v is passed", command)
		switch command.Type {
		case PutOp, AppendOp:
			return OK, ""
		case GetOp:
			return OK, kv.kv[command.Key]
		}
	}
	kv.leader.clerkIndex[command.ClerkId] = max(command.ClerkIndex, kv.leader.clerkIndex[command.ClerkId])
	var err Err
	var val string
	switch command.Type {
	case PutOp:
		err, val = kv.doPut(command)
	case GetOp:
		err, val = kv.doGet(command)
	case AppendOp:
		err, val = kv.doAppend(command)
	}
	kv.lastAppliedCommandIndex = max(commandIndex, kv.lastAppliedCommandIndex)
	kv.lastAppliedCommandTerm = max(kv.lastAppliedCommandTerm, commandTerm)
	return err, val
}

func (kv *KVServer) doPut(command *Command) (Err, string) {
	kv.kv[command.Key] = command.Value
	//if cache, ok := kv.clerkCache[command.ClerkId]; ok {
	//	cache[command.ClerkIndex] = command.Value
	//} else {
	//	m := make(map[int]string)
	//	m[command.ClerkIndex] = command.Value
	//	kv.clerkCache[command.ClerkId] = m
	//}
	DPrintf(kv, "KVServer %v Put {op %v, err %v, value %v}", kv.me, command, OK, command.Value)
	return OK, command.Value
}

func (kv *KVServer) doAppend(command *Command) (Err, string) {
	v, ok := kv.kv[command.Key]
	var value string
	if ok {
		value = v + command.Value
	} else {
		value = command.Value
	}
	kv.kv[command.Key] = value
	//if cache, ok := kv.clerkCache[command.ClerkId]; ok {
	//	cache[command.ClerkIndex] = value
	//} else {
	//	m := make(map[int]string)
	//	m[command.ClerkIndex] = value
	//	kv.clerkCache[command.ClerkId] = m
	//}
	DPrintf(kv, "KVServer %v Append {op %v, err %v, value %v}", kv.me, command, OK, value)
	return OK, value
}

func (kv *KVServer) doGet(command *Command) (Err, string) {
	v, ok := kv.kv[command.Key]
	if ok {
		command.Value = v
		//if cache, ok := kv.clerkCache[command.ClerkId]; ok {
		//	cache[command.ClerkIndex] = command.Value
		//} else {
		//	m := make(map[int]string)
		//	m[command.ClerkIndex] = command.Value
		//	kv.clerkCache[command.ClerkId] = m
		//}
		DPrintf(kv, "KVServer %v Get {op %v, err %v, value %v}", kv.me, command, OK, command.Value)
		return OK, command.Value
	} else {
		//if cache, ok := kv.clerkCache[command.ClerkId]; ok {
		//	cache[command.ClerkIndex] = ""
		//} else {
		//	m := make(map[int]string)
		//	m[command.ClerkIndex] = ""
		//	kv.clerkCache[command.ClerkId] = m
		//}
		DPrintf(kv, "KVServer %v Get {op %v, err %v, value %v}", kv.me, command, ErrNoKey, "")
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
// atomically save the Raft State along with the snapshot.
// the k/v server should snapshot when Raft's saved State exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Command{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.kv = make(map[string]string)
	kv.term = 0
	kv.isLeader = false
	kv.leader = &Leader{&sync.Map{}, make(map[int]int), list.New()}
	kv.leaderId = -1
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.clerkCache = make(map[int]map[int]string)
	kv.loadSnapshot(persister.ReadSnapshot())
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	DPrintf(kv, "%v read snapshot", kv.me)
	DPrintf(kv, "%v read snapshot end", kv.me)
	go kv.applyLoop()
	go kv.checkLogSize()
	// You may need initialization code here.

	return kv
}
func (kv *KVServer) persistBytes(a ...interface{}) []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	for i := range a {
		e.Encode(a[i])
	}
	return w.Bytes()
}
func (kv *KVServer) getSnapshotBytes() []byte {
	return kv.persistBytes(kv.lastAppliedCommandIndex, kv.lastAppliedCommandTerm, kv.kv, kv.leader.clerkIndex) // , kv.leader.clerkIndex, kv.clerkCache
}
func (kv *KVServer) loadSnapshot(snapshot []byte) {
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var lastAppliedCommandIndex int
	var lastAppliedCommandTerm int
	var data map[string]string
	var clerkIndex map[int]int
	//var clerkCache map[int]map[int]string

	if d.Decode(&lastAppliedCommandIndex) != nil || d.Decode(&lastAppliedCommandTerm) != nil || d.Decode(&data) != nil || d.Decode(&clerkIndex) != nil { //|| d.Decode(&clerkIndex) != nil || d.Decode(&clerkCache) != nil
		DPrintf(kv, "%v reload persistent error", kv.me)
	} else {
		kv.lastAppliedCommandTerm = lastAppliedCommandTerm
		kv.lastAppliedCommandIndex = lastAppliedCommandIndex
		kv.kv = data
		kv.leader.clerkIndex = clerkIndex
		//kv.clerkCache = clerkCache

	}
}

func (kv *KVServer) checkLogSize() {
	// todo 检测是否应该发送日志
	for !kv.killed() {
		kv.mu.Lock()
		if kv.maxraftstate != -1 && kv.rf.ReadLogSize() >= kv.maxraftstate {
			// 向 raft 发送快照
			DPrintf(kv, "%v send snapshot {snapshotIndex %v}", kv.me, kv.lastAppliedCommandIndex)
			kv.rf.Snapshot(kv.lastAppliedCommandIndex, kv.getSnapshotBytes())
		}
		kv.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}

func (kv *KVServer) sameCommand(nop *Option, op *Option) bool {
	if op.command.Type == nop.command.Type && op.command.Key == nop.command.Key && op.command.ClerkId == nop.command.ClerkId && op.command.ClerkIndex == nop.command.ClerkIndex {
		return true
	}
	return false
}
