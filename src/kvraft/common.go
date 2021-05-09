package kvraft

import (
	"container/list"
)

const (
	OK               = "OK"
	ErrNoKey         = "ErrNoKey"
	ErrWrongLeader   = "ErrWrongLeader"
	ErrNoTypeOpFound = "ErrNoTypeOpFound"
	Passed           = "Passed"

	PutOp    = "Put"
	AppendOp = "Append"
	GetOp    = "Get"

	Null = -1
)

type Err string

type Option struct {
	command    *Op
	notify     chan *Option
	err        Err
	clerkIndex int // clerk command's index
	clerkId    int
	raftIndex  int
	element    *list.Element
}
type Op struct {
	Key   string
	Op    string // "Put" or "Append" or "Get"
	Value string
}

func (op *Option) sameCommand(command *Op) bool {
	if command.Op == op.command.Op {
		switch command.Op {
		case GetOp:
			if command.Key == op.command.Key {
				return true
			}
		case PutOp, AppendOp:
			if command.Key == op.command.Key && command.Value == command.Value {
				return true
			}

		}
	}
	return false
}

type ICommand interface {
	buildOp() *Option
}

func max(a, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}
