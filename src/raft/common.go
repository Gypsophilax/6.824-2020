package raft

type State int

const (
	Leader    State = 0
	Candidate State = 1
	Follower  State = 2
)

type LeaderState struct {
	nextIndex  []int
	matchIndex []int
}
