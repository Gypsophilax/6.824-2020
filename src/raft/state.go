package raft

//  Log's entry
type LogEntry struct {
	Term    int
	Command string
}
