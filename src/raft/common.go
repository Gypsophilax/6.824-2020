package raft

import (
	"math/rand"
	"time"
)

const (
	Leader    int32 = 0
	Candidate int32 = 1
	Follower  int32 = 2
	Null      int   = -1
)
const (
	ElectionTimeMin int64         = 150
	ElectionTimeMax int64         = 300
	HeartbeatTime   time.Duration = time.Millisecond * 50
)

type LeaderState struct {
	nextIndex  []int
	matchIndex []int
}

type electionChanSign struct {
	Term int
}

// 获取随机时间 [250 - 400)
func getRandTime() time.Duration {
	return time.Duration((rand.Int63n(ElectionTimeMax-ElectionTimeMin) + ElectionTimeMin) * 1000 * 1000)
}

// 检查请求投票的peer日志是否更新
func checkLogUpToDate(rLogIndex int, rLogTerm int, lastLogIndex int, lastLogTerm int) bool {
	if rLogTerm > lastLogTerm {
		return false
	}
	if lastLogTerm == rLogTerm && rLogIndex > lastLogIndex {
		return false
	}
	return true
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
