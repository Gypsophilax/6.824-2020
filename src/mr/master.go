package mr

import (
	"../utils"
	"container/list"
	"encoding/gob"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

// TaskElement
type Element struct {
	MLock
	file  string
	state State // Map 的状态
	id    int
}
type MLock struct {
	Mutex sync.Mutex
}

func (l *MLock) Lock() {
	l.Mutex.Lock()
}

func (l *MLock) UnLock() {
	l.Mutex.Unlock()
}

// MapTaskElement
type MapElement struct {
	Element
	reduceElement []*ReduceElement
}

// ReduceTaskElement
type ReduceElement struct {
	Element
	mapElement *MapElement
}

type WorkerElement struct { // Master 维护的 MRWorker 数据
	MLock
	ownMapElements    *list.List   // Master 为 MRWorker 分配的Element
	ownReduceElements *list.List   // Master 为 MRWorker 分配的Element
	alive             *utils.Queue // MRWorker 向 Master 发送的心跳信息
	wState            WorkerState
	id                int32
}

func (we *WorkerElement) sendPong() {
	_ = we.alive.PutNoWait(Pong)
}

type Master struct {
	// Your definitions here.
	nReduce        int      // Map 被划分成 nReduce 个 Reduce
	mapElements    sync.Map // 处理的 FileName -> MapElement
	reduceElements sync.Map
	taskQueue      *utils.Queue // Map 和 Reduce 的任务队列
	workers        sync.Map     //  worker machine 根据唯一id进行map 映射
	count          int32        // Master 为 WorkerElement 分配的唯一标识

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
	// 是否已经存在 该Id的  WorkerElement
	if _, ok := m.workers.Load(args.WId); !ok && args.WId < 0 {
		count := atomic.AddInt32(&m.count, 1)
		we := WorkerElement{MLock{sync.Mutex{}}, list.New(), list.New(), utils.New(1), On, count}
		m.workers.Store(count, &we)
		//args.WId = count
		reply.WId = count

	}
	// todo 心跳监听机制

	// 是否有已经存在的 IMasterTask 分配给 MRWorker
	err, task := m.getAndBindTask(reply.WId)
	if err != nil {
		log.Fatal(err)
		return err
	}
	reply.WTask = task
	return err
}

// 处理 MRWorker 发送过来的心跳
func (m *Master) Heartbeat(args *HeartbeatArgs, reply *HeartbeatReply) {
	// todo
	worker, err := m.getWorkerElementById(args.WId)
	if err != nil {
		fmt.Printf("%v: %v", args.WId, err)
		return
	}
	worker.sendPong()

}

// 检查 MRWorker 是否alive
func (m *Master) checkMRWorkerAlive(we *WorkerElement) {
	for true {
		if val, err := we.alive.Get(time.Second * 10); err == nil {
			val = val.(HT)
			if val == End {
				return
			}
		} else {
			// todo 超时处理， 把 MRWorker 下线，并且将任务进行重新分配
			we.Lock()
			we.wState = Off // 把 MRWorker 下线，并且将任务进行重新分配
			we.UnLock()
			return
		}
	}
}
func (m *Master) getAndBindTask(workerid int32) (error, IWorkerTask) {
	task, err := m.taskQueue.GetNoWait()
	if err != nil {
		return err, nil
	}
	mTask := task.(IMasterTask)
	//  改变对应 Element 的状态，并绑定 MRWorker
	if err = mTask.ChangeElementAndTaskState(m, Progress); err != nil {
		return err, nil
	}

	if err = mTask.BindMRWorker(m, workerid); err != nil {
		log.Printf("bind error: %v", err)
		return err, nil

	}
	return nil, mTask.TransToWTask()
}

func (m *Master) getWorkerElementById(workerid int32) (*WorkerElement, error) {
	if worker, ok := m.workers.Load(workerid); ok {
		return worker.(*WorkerElement), nil
	}
	return nil, errors.New("id's worker doesn't exist ")
}

// 初始化 Master
func (m *Master) init(files []string) {
	m.taskQueue = utils.New(0)
	m.count = -1
	m.mapElements = sync.Map{}
	gob.Register(&MapTask{})
	for i, file := range files {
		mapTask := MapTask{Task{InFile: file, Number: i}}
		mapTask.OutFile = mapTask.BuildOutputFileNames()
		m.mapElements.Store(file, &MapElement{Element{MLock{sync.Mutex{}}, file, Idle, i}, make([]*ReduceElement, m.nReduce)})
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
// nReduce is the number of reduce todoTask to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	m.nReduce = nReduce
	m.init(files)
	m.server()
	return &m
}
