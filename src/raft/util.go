package raft

import (
	"fmt"
)

// Debugging
const Debug = 0

func DPrintf(format string, rf *Raft, a ...interface{}) (n int, err error) {
	if Debug > 0 && !rf.killed() {
		fmt.Printf(format+"\n", a...)
	}
	return
}
