package kvraft

// Put or Append
type PutAppendArgs struct {
	Key          string
	Value        string
	Op           string // "Put" or "Append"
	ClerkId      int    //ClerkId
	CommandIndex int
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err      Err
	LeaderId int
}

func (kv *KVServer) putAppendOp(args *PutAppendArgs) *Option {
	command := Op{args.Key, args.Op, args.Value}
	return &Option{&command, make(chan *Option, 1), OK, args.CommandIndex, args.ClerkId, 0, nil}
}
