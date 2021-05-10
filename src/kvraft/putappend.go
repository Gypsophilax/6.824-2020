package kvraft

// Put or Append
type PutAppendArgs struct {
	Key          string
	Value        string
	Type         string // "Put" or "Append"
	ClerkId      int    //ClerkId
	CommandIndex int
	Times        int
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err      Err
	LeaderId int
	Times    int
}

func (kv *KVServer) putAppendOp(args *PutAppendArgs) *Option {
	command := Command{args.Key, args.Type, args.Value, args.CommandIndex, args.ClerkId}
	return &Option{&command, make(chan *Option, 1), OK, 0, nil}
}
