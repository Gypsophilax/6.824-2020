package kvraft

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClerkId      int //ClerkId
	CommandIndex int
}

type GetReply struct {
	Err      Err
	Value    string
	LeaderId int
}

func (kv *KVServer) getOp(args *GetArgs) *Option {
	command := Op{args.Key, GetOp, ""}
	return &Option{&command, make(chan *Option, 1), OK, args.CommandIndex, args.ClerkId, 0, nil}
}
