package kvraft

import (
	"container/list"
)

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"

	PutOp    = "Put"
	AppendOp = "Append"
	GetOp    = "Get"

	Null = -1
)

type Err string

type Option struct {
	command   *Command
	notify    chan *Option
	err       Err
	raftIndex int
	element   *list.Element
}
type Command struct {
	Key        string
	Type       string // "Put" or "Append" or "Get"
	Value      string
	ClerkIndex int // clerk command's index
	ClerkId    int
}

type Msg struct {
	ok       bool
	leaderId int
}

func max(a, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}
