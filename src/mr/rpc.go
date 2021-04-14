package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// MRWorker 向 Master 注册
type RegisterArgs struct {
	WId int32 // worker machine的唯一标识 -1: 首次注册
}

type RegisterReply struct {
	WTasker Tasker // 任务
	WId     int32  // MRWorker 向 Master 注册的 WId
}

// MRWorker 向 Master 发送心跳
type HeartBeatArgs struct {
	WId int32
	// todo 向 Master 报告任务状态
}

type HeartBeatReply struct {
	WTasker Tasker
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
