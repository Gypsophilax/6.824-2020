package kvraft

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClerkId      int //ClerkId
	CommandIndex int
	Times        int
}

type GetReply struct {
	Err      Err
	Value    string
	LeaderId int
	Times    int
}

func (kv *KVServer) getOp(args *GetArgs) *Option {
	command := Command{args.Key, GetOp, "", args.CommandIndex, args.ClerkId}
	return &Option{&command, make(chan *Option, 1), OK, 0, nil}
}
