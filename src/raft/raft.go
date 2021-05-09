package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"sort"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"
import "../labgob"

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	LeaderId     int
	Term         int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	applyCh chan ApplyMsg

	state int32 // current node's state : Leader Follower Candidate

	// Persistent state on all servers
	currentTerm int
	votedFor    int // -1: 还没有vote
	logs        []*LogEntry

	// Volatile state on all servers
	commitIndex int
	lastApplied int

	// Volatile state on leaders
	leader *LeaderState

	// leader election
	electionTimer *time.Timer // 选举超时 Timer
	beginElection int32
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.state == Leader
	rf.mu.Unlock()

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	// 保存 Raft 的 persistent state
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	//e.Encode(rf.notDiscardCount)
	e.Encode(rf.logs)
	DPrintf("%v's persist state: term %v,votedFor %v,logSize %v", rf, rf.me, rf.currentTerm, rf.votedFor, len(rf.logs))
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
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
	// 重启之后读取持久化的状态
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []*LogEntry
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&logs) != nil {
		DPrintf("%v reload persistent error", rf, rf.me)
	} else {
		rf.mu.Lock()
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logs = logs
		DPrintf("%v's reboot state: term %v,voteFor %v,log size %v,newLog size %v", rf, rf.me, currentTerm, votedFor, len(logs), len(rf.logs))
		rf.mu.Unlock()
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PervLogIndex int
	PervLogTerm  int
	Entries      []*LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term             int
	Success          bool
	ConflictLogTerm  int
	ConflictLogIndex int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// 处理投票请求
	if rf.killed() {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("%v {term %v,state %v,votedFor %v}  handle RequestVote{args %v}",
		rf, rf.me, rf.currentTerm, rf.state, rf.votedFor, args)
	defer DPrintf("%v reply requestVote {reply %v}", rf, rf.me, reply)
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	//DPrintf("%v {term %v,state %v,votedFor %v,lastLogIndex %v,lastLogTerm %v}receive  RequestVote %v",
	//	rf.me,rf.currentTerm, rf.state, rf.votedFor, lastLogIndex, lastLogTerm, args)
	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm { // && (rf.state == Candidate || rf.state == Leader)
		DPrintf("%v {state %v} convert to follower", rf, rf.me, rf.state)
		rf.convertToFollower(args.Term)
		reply.Term = rf.currentTerm
	}

	lastLogIndex := len(rf.logs)
	lastLogTerm := 0
	if lastLogIndex > 0 {
		lastLogTerm = rf.logs[lastLogIndex-1].Term
	}
	DPrintf("%v {lastLogIndex %v,lastLogTerm %v}  handle RequestVote{args %v}", rf, rf.me, lastLogIndex, lastLogTerm, args)
	// args.Term >= rf.currentTerm && 不需要，与上述条件互斥
	upToDate := checkLogUpToDate(lastLogIndex, lastLogTerm, args.LastLogIndex, args.LastLogTerm)
	// Follower 只有在未投票并且Term比自己大，日志比自己新的情况下才会投票
	// 如果Term小于自己当前Term，说明接收到了过期的投票信息，直接忽略

	// Candidate 和 Leader 回到 Follower
	if rf.state == Follower && (rf.votedFor == Null || rf.votedFor == args.CandidateId) && upToDate {
		rf.votedFor = args.CandidateId
		rf.persist()
		reply.VoteGranted = true
		rf.electionTimer.Reset(getRandTime()) // 投票，重置选举超时
		DPrintf("%v reset electionTimer for vote", rf, rf.me)
	}

}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	// 处理心跳或添加日志请求
	if rf.killed() {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.Success = false
	DPrintf("%v {term %v, state %v, logSize %v, commitIndex %v} handle AppendEntries {args %v}", rf, rf.me, rf.currentTerm, rf.state, len(rf.logs), rf.commitIndex, args)
	defer DPrintf("%v reply AppendEntries {reply %v}", rf, rf.me, reply)
	if args.Term < rf.currentTerm {
		return
	}
	if args.Term == rf.currentTerm {
		switch rf.state {
		case Leader:
			DPrintf("There already have a leader", rf)
		case Candidate:
			DPrintf("%v {state %v}  convert to follower", rf, rf.me, rf.state)
			rf.convertToFollower(args.Term)
		}
	}
	if args.Term > rf.currentTerm {
		DPrintf("%v {state %v} convert to follower", rf, rf.me, rf.state)
		rf.convertToFollower(args.Term)
		reply.Term = rf.currentTerm
	}
	// 如果 Leader 和 Candidate 接受了这个 AppendEntries 说明同意它的Leader 身份，可以重置选举时间
	if rf.state != Follower { // || rf.commitIndex > args.LeaderCommit
		DPrintf("%v reject this AppendEntries", rf, rf.me)
		reply.Success = false // ?
		return
	}
	DPrintf("%v reset electionTimer for current leader's AppendEntries", rf, rf.me)
	rf.electionTimer.Reset(getRandTime()) // 重置选举时间

	reply.Success = true
	// 不区分是否是心跳
	if args.PervLogIndex > len(rf.logs) { // 收到的log和已有log不相交
		reply.Success = false
		reply.ConflictLogIndex = len(rf.logs) // conflictLogIndex 应该特殊处理
		reply.ConflictLogTerm = Null
		return
	}
	// 如果 PervLogIndex=0 ，说明是初始状态，不必检查。
	if args.PervLogIndex > 0 { // prevLogIndex所在log冲突
		if entry := rf.logs[args.PervLogIndex-1]; entry.Term != args.PervLogTerm {
			reply.Success = false
			reply.ConflictLogTerm = entry.Term
			reply.ConflictLogIndex = rf.getConflictIndex(reply.ConflictLogTerm)
			//rf.logs = append(rf.logs[:args.PervLogIndex-1]) //删除冲突之后的log
			DPrintf("Truncate %v's log. size{new %v} conflict{index %v, term %v, leader's term %v}", rf, rf.me, len(rf.logs), args.PervLogIndex, entry.Term, args.PervLogTerm)
			//rf.persist()
			return
		}

	}
	// 检查 prevLogIndex 之后的日志是否存在冲突，有冲突则删除
	nextIndex := args.PervLogIndex + 1
	leaderIndex := args.PervLogIndex + len(args.Entries)
	maxIndex := min(len(rf.logs), leaderIndex)
	for nextIndex <= maxIndex {
		if entry := rf.logs[nextIndex-1]; entry.Term != args.Entries[nextIndex-args.PervLogIndex-1].Term {
			rf.logs = append(rf.logs[:nextIndex-1])
			rf.persist()
			DPrintf("Truncate %v's log. size{new %v} conflict{index %v, term %v, leader's term %v}", rf, rf.me, len(rf.logs), nextIndex, entry.Term, args.Entries[nextIndex-args.PervLogIndex-1].Term)
			break
		}
		nextIndex++
	}
	DPrintf("Current log size {leader(%v) %v,follower(%v) %v, followerTerm %v}. {nextIndex %v}", rf, args.LeaderId, leaderIndex, rf.me, len(rf.logs), rf.currentTerm, nextIndex)
	if leaderIndex >= nextIndex {
		rf.logs = append(rf.logs, args.Entries[nextIndex-args.PervLogIndex-1:]...)
		rf.persist()
	}
	DPrintf("%v done AppendEntries{logSize %v, leaderCommit %v, rf.commitIndex %v}", rf, rf.me, len(rf.logs), args.LeaderCommit, rf.commitIndex)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, leaderIndex)
		DPrintf("Update follower %v's commitIndex to %v {term %v}", rf, rf.me, rf.commitIndex, rf.currentTerm)
	}

}

func (rf *Raft) getConflictIndex(conflictTerm int) int {
	for i := range rf.logs {
		if rf.logs[i].Term == conflictTerm {
			return i + 1
		}
	}
	return 1
}

func (rf *Raft) convertToFollower(term int) {
	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = Null
	rf.persist()
	rf.leader = nil
	rf.beginElectionLoop()
}

func (rf *Raft) convertToCandidate() {
	rf.currentTerm = rf.currentTerm + 1
	rf.state = Candidate
	rf.votedFor = rf.me
	rf.persist()
	rf.leader = nil
}
func (rf *Raft) convertToLeader() {
	rf.state = Leader
	rf.persist()
	rf.leader = &LeaderState{}
	peerSize := len(rf.peers)
	lastLogIndex := len(rf.logs)
	nextIndex := make([]int, peerSize)
	matchIndex := make([]int, peerSize)
	for i := 0; i < peerSize; i++ {
		nextIndex[i] = lastLogIndex + 1
		matchIndex[i] = 0
	}
	matchIndex[rf.me] = len(rf.logs)
	rf.leader.nextIndex = nextIndex
	rf.leader.matchIndex = matchIndex
	DPrintf("Leader %v's init {nextIndex %v, matchIndex %v, logSize %v}", rf, rf.me, rf.leader.nextIndex, rf.leader.matchIndex, len(rf.logs))
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// Send AppendEntries to peer
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index = len(rf.logs) + 1 // todo 客户端重试同一个命令
	term = rf.currentTerm
	isLeader = rf.state == Leader
	if isLeader && !rf.killed() {
		// 追加日志
		DPrintf("Term %v, %v start receive command {index %v, command %v}", rf, term, rf.me, index, command)
		rf.logs = append(rf.logs, &LogEntry{term, command})
		rf.persist()
		rf.leader.matchIndex[rf.me]++
		rf.leader.nextIndex[rf.me] = rf.leader.matchIndex[rf.me] + 1
		//rf.leaderAppendLog(command)
	}
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// 初始化 Raft
func (rf *Raft) init(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) {
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logs = make([]*LogEntry, 0)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.beginElection = 0
	rf.applyCh = applyCh
	rf.electionTimer = time.NewTimer(getRandTime())
}

// leader 选举 loop
func (rf *Raft) electionLoop() {

	for !rf.killed() {
		<-rf.electionTimer.C // 说明选举超时触发
		// 重置 electionTimer
		rf.mu.Lock()
		if rf.state == Leader {
			DPrintf("%v is leader, shouldnt continue leader election", rf, rf.me)
			rf.beginElection = 0
			rf.mu.Unlock()
			return
		}
		rf.electionTimer.Reset(getRandTime())
		go func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			me := rf.me
			// state change
			DPrintf("%v {state %v} convert to candidate", rf, rf.me, rf.state)
			rf.convertToCandidate()
			term := rf.currentTerm
			lastLogIndex := len(rf.logs)
			lastLogTerm := 0
			if lastLogIndex > 0 {
				lastLogTerm = rf.logs[lastLogIndex-1].Term
			}
			peerSize := len(rf.peers)
			DPrintf("%v begin term %v's leader election at time %v {majority %v, logSize %v}", rf, me, term, time.Now().UnixNano()/1e6, (peerSize+1)>>1, len(rf.logs))
			// 进行选举
			var voteCount int64 = 1
			for i := 0; i < peerSize; i++ {
				if i == me {
					continue
				}
				DPrintf("%v call %v.RequestVote {args: term %v, lastLogIndex %v, lastLogTerm %v}", rf, me, i, term, lastLogIndex, lastLogTerm)
				go rf.doElection(term, me, lastLogIndex, lastLogTerm, i, (peerSize+1)>>1, &voteCount)
			}
		}()
		rf.mu.Unlock()

	}

}

// 进行选举逻辑
func (rf *Raft) doElection(term, me, lastLogIndex, lastLogTerm, server, majority int, voteCount *int64) {
	if rf.killed() {
		return
	}
	args := RequestVoteArgs{term, me, lastLogIndex, lastLogTerm}
	reply := RequestVoteReply{}
	if rf.sendRequestVote(server, &args, &reply) {
		if rf.killed() {
			return
		}
		rf.mu.Lock()
		DPrintf("%v {currentTerm %v, state %v} receive %v's RequestVote reply {args %v, reply %v}", rf, rf.me, rf.currentTerm, rf.state, server, args, reply)
		if rf.currentTerm == term && rf.state == Candidate {
			if reply.Term > rf.currentTerm {
				DPrintf("%v {state %v} convert to follower", rf, rf.me, rf.state)
				rf.convertToFollower(reply.Term)
				//rf.electionTimer.Reset(getRandTime())
			} else if reply.Term < rf.currentTerm {
				DPrintf("This RequestVote reply is passed. {args %v ,reply %v}", rf, args, reply)
			} else if reply.VoteGranted { // 如果 server 还在当前任期，消息未过期 // 未过期的返回消息reply.Term不会小于term，所以只能等于term // rf.state == Candidate && rf.currentTerm == reply.Term &&
				voteCount := atomic.AddInt64(voteCount, 1)
				DPrintf("%v's {term %v} vote count %v", rf, rf.me, rf.currentTerm, voteCount)
				if voteCount == int64(majority) { // 变为 Leader
					DPrintf("Term %v,%v become to leader", rf, term, me)
					rf.becomeLeader()
				}
			}
		}
		rf.mu.Unlock()
	}
}

// Candidate to Leader, 已经在 rf.doElection 时持有锁, 所以无需加锁
func (rf *Raft) becomeLeader() {
	if rf.killed() {
		return
	}
	// 转换状态
	DPrintf("%v {state %v} convert to Leader", rf, rf.me, rf.state)
	rf.convertToLeader()
	// 在选举成功之后立即添加一个空log
	//rf.logs = append(rf.logs, &LogEntry{rf.currentTerm, nil})
	//rf.persist()
	//rf.leader.matchIndex[rf.me]++
	//rf.leader.nextIndex[rf.me] = rf.leader.matchIndex[rf.me] + 1
	//DPrintf("%v commit nil at index %v in term %v", rf, rf.me, rf.leader.matchIndex[rf.me], rf.currentTerm)
	// 定时发送 AppendEntries
	go rf.appendEntriesLoop()

}

// 定时发送 AppendEntries
func (rf *Raft) appendEntriesLoop() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}
		//DPrintf("%v reset electionTimer for it's own AppendEntriesLoop", rf, rf.me)
		peerSize := len(rf.peers)
		term := rf.currentTerm

		me := rf.me
		leaderCommit := rf.commitIndex
		DPrintf("%v's appendEntriesLoop {peerSize %v, nextIndex %v, matchIndex %v, term %v}", rf, me, peerSize, rf.leader.nextIndex, rf.leader.matchIndex, rf.currentTerm)
		for i := 0; i < peerSize; i++ {
			if i == me {
				continue
			}
			nextIndex := rf.leader.nextIndex[i]
			prevLogIndex := 0
			prevLogTerm := 0
			entries := make([]*LogEntry, 0)
			if nextIndex > 1 {
				prevLogIndex = nextIndex - 1
				prevLogTerm = rf.logs[prevLogIndex-1].Term
			}
			if len(rf.logs) >= nextIndex {
				entries = append(entries, rf.logs[prevLogIndex:]...)
			}
			DPrintf("Leader call %v.AppendEntries {args: term %v, leaderId %v, prevLogIndex %v, prevLogTerm %v, leaderCommit %v, logSize %v, appendSize %v, logs %v}", rf, i, term, me, prevLogIndex, prevLogTerm, leaderCommit, len(rf.logs), len(entries), entries)
			go rf.doSendAppendEntries(term, me, prevLogIndex, prevLogTerm, leaderCommit, i, entries)
		}

		rf.mu.Unlock()
		time.Sleep(HeartbeatTime)
	}
}
func (rf *Raft) doSendAppendEntries(term, leaderId, prevLogIndex, prevLogTerm, leaderCommit, server int, entries []*LogEntry) {
	if rf.killed() {
		return
	}
	args := AppendEntriesArgs{term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit}
	reply := AppendEntriesReply{}
	DPrintf("Leader call %v.AppendEntries{args %v}", rf, server, &args)
	if rf.sendAppendEntries(server, &args, &reply) {
		rf.mu.Lock()
		DPrintf("%v {currentTerm %v, state %v} receive  %v's AppendEntries reply {args %v, reply %v}", rf, rf.me, rf.currentTerm, rf.state, server, args, reply)
		if rf.currentTerm == term && rf.state == Leader {
			if reply.Term > rf.currentTerm {
				DPrintf("%v {state %v} convert to follower", rf, rf.me, rf.state)
				rf.convertToFollower(reply.Term)
			} else if reply.Term < rf.currentTerm {
				DPrintf("This AppendEntries reply is passed. {args %v ,reply %v}", rf, args, reply)
			} else {
				if !reply.Success {
					if reply.ConflictLogTerm != Null {
						DPrintf("%v log size is %v, receive %v's append conflict {conflict index %v, conflict term %v} {old nextIndex %v, matchIndex %v, new nextIndex %v} ",
							rf, rf.me, len(rf.logs), server, reply.ConflictLogIndex, reply.ConflictLogTerm, rf.leader.nextIndex[server], rf.leader.matchIndex[server], rf.getConflictIndex(reply.ConflictLogTerm))
						if rf.leader.matchIndex[server] > rf.getConflictIndex(reply.ConflictLogTerm) {
							DPrintf("This reply is passed {args %v, reply %v, matchIndex %v}", rf, args, reply, rf.leader.matchIndex[server])
						} else {
							rf.leader.nextIndex[server] = rf.getConflictIndex(reply.ConflictLogTerm)
						} // 和matchIndex 取最大值？
					} else {
						// 慢速 nextIndex
						DPrintf("This reply dont have conflictTerm", rf)
						//rf.leader.nextIndex[server] = reply.ConflictLogIndex 过时消息处理不当
						if rf.leader.matchIndex[server] > reply.ConflictLogIndex {
							DPrintf("This reply is passed {args %v, reply %v, matchIndex %v}", rf, args, reply, rf.leader.matchIndex[server])
						} else {
							rf.leader.nextIndex[server] = reply.ConflictLogIndex
						} // 和matchIndex 取最大值？
					}
				} else {
					rf.leader.matchIndex[server] = max(prevLogIndex+len(entries), rf.leader.matchIndex[server]) // matchIndex 不可能会减小？？
					rf.leader.nextIndex[server] = rf.leader.matchIndex[server] + 1
					//atomic.StoreInt64(nextCommitIndex, int64(max(int(*nextCommitIndex), rf.leader.matchIndex[server]))) //   update commitIndex is
					// update commitIndex
					peerSize := len(rf.leader.matchIndex)
					matchIndex := make([]int, peerSize)
					copy(matchIndex, rf.leader.matchIndex)
					sort.Ints(matchIndex)

					nextCommitIndex := matchIndex[peerSize-(peerSize+1)>>1]
					//DPrintf("leader's matchIndex: %v,nextCommitIndex; %v", matchIndex, nextCommitIndex)
					for nextCommitIndex > rf.commitIndex && rf.logs[nextCommitIndex-1].Term != rf.currentTerm {
						//DPrintf("nextCommitIndex--")
						nextCommitIndex--
					}
					nextCommitIndex = max(nextCommitIndex, matchIndex[0])
					DPrintf("Leader update %v's matchIndex %v and nextIndex %v. {matchIndex %v, nextIndex %v,nextCommitIndex %v, term %v, leaderCommit %v}", rf, server, rf.leader.matchIndex[server], rf.leader.nextIndex[server], rf.leader.matchIndex, rf.leader.nextIndex, nextCommitIndex, rf.currentTerm, rf.commitIndex)

					if rf.commitIndex < nextCommitIndex {
						rf.commitIndex = nextCommitIndex
						DPrintf("Update leader %v's {commitIndex %v, matchIndex %v, term %v}", rf, rf.me, rf.commitIndex, rf.leader.matchIndex, rf.currentTerm)
					}
				}
			}

		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) commitLoop() {
	for !rf.killed() {
		rf.mu.Lock()
		commitIndex := rf.commitIndex
		applied := rf.lastApplied
		rf.mu.Unlock()
		//DPrintf("%v is alive", rf.me)
		//DPrintf("%v {commitIndex %v, lastApplied %v, logSize %v}", rf.me, rf.commitIndex, rf.lastApplied, len(rf.logs))
		nextApply := applied + 1
		for commitIndex >= nextApply {
			entry := rf.logs[nextApply-1]
			DPrintf("%v commit log to server in current term %v {command %v, index %v, term %v}", rf, rf.me, rf.currentTerm, entry.Command, nextApply, entry.Term)
			rf.mu.Lock()
			term := rf.currentTerm
			rf.mu.Unlock()
			rf.applyCh <- ApplyMsg{true, entry.Command, nextApply, -1, term}
			nextApply++
		}
		rf.mu.Lock()
		rf.lastApplied = nextApply - 1
		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)

	}
	DPrintf("%v is dead", rf, rf.me)
}

func (rf *Raft) beginElectionLoop() {
	if atomic.CompareAndSwapInt32(&rf.beginElection, 0, 1) {
		DPrintf("%v beign leader election loop", rf, rf.me)
		rf.electionTimer.Reset(getRandTime())
		go rf.electionLoop()
	}
}

//func (rf *Raft) updateCommitIndexLoop() {
//	for !rf.killed() {
//		rf.mu.Lock()
//		if rf.state == Leader {
//			peerSize := len(rf.leader.matchIndex)
//			matchIndex := make([]int, peerSize)
//			copy(matchIndex, rf.leader.matchIndex)
//			sort.Ints(matchIndex)
//
//			nextCommitIndex := matchIndex[peerSize-(peerSize+1)>>1]
//			//DPrintf("leader's matchIndex: %v,nextCommitIndex; %v", matchIndex, nextCommitIndex)
//			for nextCommitIndex > rf.commitIndex && rf.logs[nextCommitIndex-1].Term != rf.currentTerm {
//				//DPrintf("nextCommitIndex--")
//				nextCommitIndex--
//			}
//			if rf.commitIndex < nextCommitIndex {
//				rf.commitIndex += min(nextCommitIndex-rf.commitIndex, 10)
//				DPrintf("update leader %v's commitIndex :%v ,matchIndex: %v", rf.me, rf.commitIndex, rf.leader.matchIndex)
//			}
//		}
//		rf.mu.Unlock()
//		time.Sleep(100 * time.Millisecond)
//	}
//}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}

	// Your initialization code here (2A, 2B, 2C).
	// init raft
	rf.init(peers, me, persister, applyCh)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.beginElectionLoop()
	go rf.commitLoop()
	//go rf.updateCommitIndexLoop()
	DPrintf("Raft init success", rf)

	return rf
}
