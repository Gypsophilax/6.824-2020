package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Master struct {
	// Your definitions here.
	nReduce  int
	mapFiles []string
	taskChan chan *MapTask
	workers  []MRWorker
	task     Tasker
}

var count = 0
var countSync = sync.Mutex{}

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

func (m *Master) Register(args *RegisterArgs, reply *RegisterReply) error {
	countSync.Lock()
	defer countSync.Unlock()
	nWorker := MRWorker{id: count} // todo 是否需要在注册的时候将任务进行分配，如果分配使用了chan会一直阻塞
	m.workers = append(m.workers, nWorker)
	reply.Id = count
	reply.Tasker = <-m.taskChan
	m.task = reply.Tasker
	count++
	go func() {
		time.Sleep(100000)
		m.task.GetState()
	}()
	return nil
}

func (m *Master) init(files []string) {
	m.taskChan = make(chan *MapTask, 10)
	for i, file := range files {
		mapTask := MapTask{Task{InFile: file, State: Idle, Number: i}}
		m.taskChan <- &mapTask
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
