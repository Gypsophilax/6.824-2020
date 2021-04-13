package mr

import (
	"../utils"
	"encoding/gob"
	"log"
	"sync"
	"sync/atomic"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Element struct {
	file  string
	state State // Map 的状态
	id    int
}
type MapElement struct {
	Element
	reduceElement []ReduceElement
	lock          sync.Mutex
}
type ReduceElement struct {
	Element
	mapElement *MapElement
	lock       sync.Mutex
}

type WorkerElement struct {
	task      *Task
	taskQueue *utils.Queue // linkedList
	id        int32
}
type Master struct {
	// Your definitions here.
	nReduce        int                    // Map 被划分成 nReduce 个 Reduce
	mapElements    map[string]*MapElement // 处理的 FileName -> MapElement
	reduceElements map[string]*ReduceElement
	taskQueue      *utils.Queue // Map 和 Reduce 的任务队列
	workers        sync.Map     //  worker machine 根据唯一id进行map 映射
	count          int32        // Master 为 WorkerElerment 分配的唯一标识

}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// MRWorker 首次链接注册 Master，
func (m *Master) Register(args *RegisterArgs, reply *RegisterReply) error {
	var rErr error = nil
	// 是否已经存在 该Id的  WorkerElement
	if _, ok := m.workers.Load(args.Id); !ok && args.Id < 0 {
		count := atomic.AddInt32(&m.count, 1)
		we := WorkerElement{id: count} // todo 是否需要在注册的时候将任务进行分配，如果分配使用了chan会一直阻塞
		m.workers.Store(args.Id, we)
		reply.Id = count

	}
	// 是否有已经存在的 Tasker 分配给 MRWorker
	if noWait, err := m.taskQueue.GetNoWait(); err != utils.ErrEmptyQueue {
		tasker := noWait.(Tasker)
		// todo 改变对应 Element 的状态
		rErr = tasker.ChangeState(m, Progress)
		reply.WTasker = tasker

	}
	if rErr != nil {
		log.Fatal(rErr)
	}

	return rErr
}

// 初始化 Master
func (m *Master) init(files []string) {
	m.taskQueue = utils.New(0)
	m.count = 0
	m.mapElements = make(map[string]*MapElement)
	gob.Register(&MapTask{})
	for i, file := range files {
		mapTask := MapTask{Task{InFile: file, State: Idle, Number: i}}
		m.mapElements[file] = &MapElement{Element{file: file, state: Idle, id: i}, make([]ReduceElement, m.nReduce), sync.Mutex{}}
		_ = m.taskQueue.PutNoWait(&mapTask)
	}
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	m.nReduce = nReduce
	m.init(files)
	m.server()
	return &m
}
