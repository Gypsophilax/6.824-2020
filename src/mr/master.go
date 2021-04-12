package mr

import (
	"../utils"
	"encoding/gob"
	"log"
	"sync"
	"sync/atomic"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Master struct {
	// Your definitions here.
	nReduce   int
	mapFiles  []string
	taskQueue *utils.Queue
	workers   *sync.Map //  worker machine 根据唯一id进行map 映射
	count     int32
}

var timer = time.NewTimer(time.Second * 3) //channel 的超时设置
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

	// 如果 MRWorker 不存在，就添加，如果存在的话就直接分配任务
	if _, ok := m.workers.Load(args.Id); !ok {
		count := atomic.AddInt32(&m.count, 1)
		nWorker := MRWorker{id: count} // todo 是否需要在注册的时候将任务进行分配，如果分配使用了chan会一直阻塞
		m.workers.Store(args.Id, nWorker)
		reply.Id = count

	}
	noWait, _ := m.taskQueue.GetNoWait()
	reply.WTasker = noWait.(*MapTask)
	return nil
}

func (m *Master) init(files []string) {
	m.taskQueue = utils.New(0)
	m.workers = new(sync.Map)
	m.count = 0
	gob.Register(&MapTask{})
	for i, file := range files {
		mapTask := MapTask{Task{InFile: file, State: Idle, Number: i}}
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
