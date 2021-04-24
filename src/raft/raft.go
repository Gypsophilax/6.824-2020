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
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"

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
	Term    int
	Success bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// 处理投票请求
	rf.mu.Lock()
	defer rf.mu.Unlock()
	lastLogIndex := len(rf.logs)
	lastLogTerm := 0
	if lastLogIndex > 0 {
		lastLogTerm = rf.logs[lastLogIndex-1].Term
	}
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	// args.Term >= rf.currentTerm && 不需要，与上述条件互斥
	upToDate := args.Term >= rf.currentTerm && checkLogUpToDate(lastLogIndex, lastLogTerm, args.LastLogIndex, args.LastLogTerm)
	// Follower 只有在未投票并且Term比自己大，日志比自己新的情况下才会投票
	// 如果Term小于自己当前Term，说明接收到了过期的投票信息，直接忽略
	if rf.state == Follower && upToDate && (rf.votedFor == -1 || rf.votedFor == args.CandidateId) {
		rf.electionTimer.Reset(getRandTime()) // 投票，重置选举超时

		rf.votedFor = args.CandidateId
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		return
	}
	//
	if (rf.state == Candidate || rf.state == Leader) && upToDate {
		rf.electionTimer.Reset(getRandTime()) // 投票，重置选举超时

		rf.state = Follower
		rf.votedFor = args.CandidateId
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		rf.leader = nil
		return
	}

}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	// todo 处理心跳或添加日志请求
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	if rf.state == Leader && args.Term == rf.currentTerm {
		DPrintf("brain split")
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	rf.electionTimer.Reset(getRandTime()) // 重置选举时间
	if rf.state == Leader && args.Term > rf.currentTerm {
		rf.state = Follower
		rf.currentTerm = args.Term
		rf.leader = nil
	}
	if rf.state == Candidate && args.Term >= rf.currentTerm {
		rf.state = Follower
		reply.Success = true

	}
	reply.Term = rf.currentTerm
	reply.Success = true
	if args.PervLogIndex == 0 || args.PervLogTerm == 0 { // 是心跳，无需进行其他处理
		return
	}

	// 非心跳
	if entry := rf.logs[args.PervLogIndex-1]; entry == nil || entry.Term != args.PervLogTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	nextIndex := args.PervLogIndex + 1
	if entry := rf.logs[nextIndex-1]; entry != nil && entry.Term != args.Term {
		rf.logs = append(rf.logs[:nextIndex-1])
	}
	rf.logs = append(rf.logs, args.Entries...)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.logs))
	}

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
	rf.electionTimer = time.NewTimer(getRandTime())
}

// leader 选举 loop
func (rf *Raft) electionLoop() {
	for !rf.killed() {
		<-rf.electionTimer.C // 说明选举超时触发
		// 重置 electionTimer
		rf.mu.Lock()
		rf.electionTimer.Reset(getRandTime())

		me := rf.me
		// state change
		rf.currentTerm = rf.currentTerm + 1
		rf.state = Candidate
		rf.votedFor = me

		term := rf.currentTerm
		lastLogIndex := len(rf.logs)
		lastLogTerm := 0
		if lastLogIndex > 0 {
			lastLogTerm = rf.logs[lastLogIndex-1].Term
		}
		peerSize := len(rf.peers)
		DPrintf("term: %v, %v begin leader election,majority is %v", term, me, (peerSize+1)>>1)
		rf.mu.Unlock()
		// 进行选举
		var voteCount int64 = 1
		for i := 0; i < peerSize; i++ {
			if i == me {
				continue
			}
			go rf.doElection(term, me, lastLogIndex, lastLogTerm, i, (peerSize+1)>>1, &voteCount)
		}

	}

}

// 进行选举逻辑
func (rf *Raft) doElection(term, me, lastLogIndex, lastLogTerm, server, majority int, voteCount *int64) {
	args := RequestVoteArgs{term, me, lastLogIndex, lastLogTerm}
	reply := RequestVoteReply{}
	DPrintf("term: %v, %v request %v to vote", term, me, server)
	if rf.sendRequestVote(server, &args, &reply) {
		DPrintf("term: %v ,%v vote to %v is %v", term, server, me, reply)
		rf.mu.Lock()
		if rf.currentTerm == term && rf.state == Candidate { // 如果 server 还在当前任期，消息未过期
			if reply.Term > term {
				rf.state = Follower
				rf.currentTerm = reply.Term
			} else if reply.Term == term && reply.VoteGranted { // 未过期的返回消息reply.Term不会小于term，所以只能等于term
				voteCount := atomic.AddInt64(voteCount, 1)
				if voteCount == int64(majority) { // 变为 Leader
					DPrintf("term: %v,%v become to leader", term, me)
					rf.becomeLeader()
				}
			}
		}
		rf.mu.Unlock()
	}

}

// Candidate to Leader, 已经在 rf.doElection 时持有锁, 所以无需加锁
func (rf *Raft) becomeLeader() {

	// 转换状态
	rf.state = Leader
	rf.leader = &LeaderState{}
	peerSize := len(rf.peers)
	lastLogIndex := len(rf.logs)
	nextIndex := make([]int, peerSize)
	matchIndex := make([]int, peerSize)
	for i := 0; i < peerSize; i++ {
		nextIndex[i] = lastLogIndex + 1
		matchIndex[i] = 0
	}
	rf.leader.nextIndex = nextIndex
	rf.leader.matchIndex = matchIndex

	// 定时发送 AppendEntries
	go rf.appendEntriesLoop(int64(rf.commitIndex))

}

// todo 定时发送 AppendEntries
func (rf *Raft) appendEntriesLoop(nextCommitIndex int64) {
	for !rf.killed() && atomic.LoadInt32(&rf.state) == Leader {
		rf.mu.Lock()
		rf.electionTimer.Reset(getRandTime())
		peerSize := len(rf.peers)
		term := rf.currentTerm

		// 更新 commitIndex
		for nextCommitIndex > int64(rf.commitIndex) && rf.logs[nextCommitIndex].Term != term {
			nextCommitIndex--
		}
		rf.commitIndex = int(nextCommitIndex)
		me := rf.me
		leaderCommit := rf.commitIndex
		for i := 0; i < peerSize; i++ {
			if i == me {
				continue
			}
			nextIndex := rf.leader.nextIndex[i]
			prevLogIndex := 0
			prevLogTerm := 0
			entries := make([]*LogEntry, 0)
			if len(rf.logs) >= nextIndex {
				prevLogIndex = nextIndex - 1
				prevLogTerm = rf.logs[prevLogIndex-1].Term
				entries = append(entries, rf.logs[prevLogIndex:]...)
			}

			go rf.doSendAppendEntries(term, me, prevLogIndex, prevLogTerm, leaderCommit, i, entries, &nextCommitIndex)
		}

		rf.mu.Unlock()
		time.Sleep(HeartbeatTime)
	}
}
func (rf *Raft) doSendAppendEntries(term, leaderId, prevLogIndex, prevLogTerm, leaderCommit, server int, entries []*LogEntry, nextCommitIndex *int64) {
	args := AppendEntriesArgs{term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit}
	reply := AppendEntriesReply{}
	DPrintf("term: %v,%v send to %v appendEntries %v", term, leaderId, server, args)
	if rf.sendAppendEntries(server, &args, &reply) {
		rf.mu.Lock()
		if rf.currentTerm == term && rf.state == Leader {
			DPrintf("term: %v,%v receive  %v's reply %v", term, leaderId, server, reply)
			if !reply.Success {
				rf.leader.nextIndex[server]--
			} else {
				rf.leader.nextIndex[server]++
				rf.leader.matchIndex[server] = prevLogTerm + len(entries)
				atomic.StoreInt64(nextCommitIndex, int64(min(int(*nextCommitIndex), rf.leader.matchIndex[server])))
			}
		}
		rf.mu.Unlock()
	}
}

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
	go rf.electionLoop()
	DPrintf("Raft init success")
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
