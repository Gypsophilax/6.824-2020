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
	WTask IWorkerTask // 任务
	WId   int32       // MRWorker 向 Master 注册的 WId
}

// MRWorker 完成该任务之后向 Master 汇报
type TaskDoneArgs struct {
	Task IMasterTask
}
type TaskDoneReply struct {
	Task IWorkerTask
}

// MRWorker 向 Master 发送心跳
type HeartbeatArgs struct {
	WId int32
	// todo 向 Master 报告任务状态
	DoneTask []IMasterTask
	ErrTask  []IMasterTask
}

type HeartbeatReply struct {
	WTask    IWorkerTask   // 新任务
	DoneTask []IWorkerTask //
	ErrTask  []IWorkerTask
	State    WorkerState // MRWorker 在 Master 中的状态
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
