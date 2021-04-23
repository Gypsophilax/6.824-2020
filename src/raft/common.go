package raft

import (
	"math/rand"
	"time"
)

type State int

const (
	Leader    State = 0
	Candidate State = 1
	Follower  State = 2
)
const (
	ElectionTimeMin int64 = 250
	ElectionTimeMax int64 = 400
	HeartbeatTime   int   = 150
)

type LeaderState struct {
	nextIndex  []int
	matchIndex []int
}

type electionChanSign struct {
	Term int
}

// 获取随机时间
func getRandTime() time.Duration {
	return time.Duration((rand.Int63n(ElectionTimeMax-ElectionTimeMin) + ElectionTimeMin) * 1000 * 1000)
}

// 检查请求投票的peer日志是否更新
func checkLogUpToDate(rLogIndex int, rLogTerm int, lastLogIndex int, lastLogTerm int) bool {
	if rLogTerm < lastLogTerm {
		return true
	} else if rLogTerm > lastLogTerm {
		return false
	} else if rLogIndex > lastLogIndex {
		return false
	} else {
		return true
	}
}
func min(a, b int) int {
	if a > b {
		return b
	} else {
		return a
	}
}
func max(a, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}
