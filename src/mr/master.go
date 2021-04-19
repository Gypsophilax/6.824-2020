package mr

import (
	"../utils"
	"container/list"
	"encoding/gob"
	"errors"
	"fmt"
	"log"
	"strconv"
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
	infile  string
	outfile []string
	//reduceElement []*ReduceElement
}

func (me *MapElement) BuildFileNames(m *Master) []string {
	var filenames []string
	s := MapFilePrefix + strconv.Itoa(me.id) + "-"
	for i := 0; i < m.nReduce; i++ {
		filenames = append(filenames, s+strconv.Itoa(i))
	}
	return filenames
}
func (me *MapElement) BuildTask() *MapTask {
	return &MapTask{me.id, me.infile, me.outfile}
}

// ReduceTaskElement
type ReduceElement struct {
	Element
	infile  []string
	outfile string
	//mapElement *MapElement
}

func (re *ReduceElement) BuildFileNames(m *Master) []string {
	var filenames []string
	for i := 0; i < m.nMap; i++ {
		filenames = append(filenames, MapFilePrefix+strconv.Itoa(i)+"-"+strconv.Itoa(re.id))
	}
	return filenames
}

func (re *ReduceElement) BuildTask() *ReduceTask {
	return &ReduceTask{re.id, re.infile, re.outfile}
}

type WorkerElement struct { // Master 维护的 MRWorker 数据
	MLock
	ownMapElements    *list.List   // Master 为 MRWorker 分配的Element
	ownReduceElements *list.List   // Master 为 MRWorker 分配的Element
	alive             *utils.Queue // MRWorker 向 Master 发送的心跳信息
	wState            WorkerState
	id                int32
}

func (we *WorkerElement) sendSign(sign HT) {
	_ = we.alive.PutNoWait(sign)
}

type Master struct {
	// Your definitions here.
	nReduce             int // Map 被划分成 nReduce 个 Reduce
	nMap                int
	mapElements         *sync.Map // 处理的 FileName -> MapElement
	reduceElements      *sync.Map
	taskQueue           *utils.Queue // Map 和 Reduce 的任务队列
	workers             *sync.Map    //  worker machine 根据唯一id进行map 映射
	count               int32        // Master 为 WorkerElement 分配的唯一标识
	doneMapTaskCount    int32
	doneReduceTaskCount int32
	workerCount         int32
	complete            int32
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
	count := atomic.AddInt32(&m.count, 1)
	if atomic.LoadInt32(&m.complete) == 1 {
		reply.WTask = &ExitTask{}
		reply.WId = count
		return nil
	}
	if _, ok := m.workers.Load(args.WId); !ok && args.WId < 0 {
		we := WorkerElement{MLock{sync.Mutex{}}, list.New(), list.New(), utils.New(1), On, count}
		m.workers.Store(count, &we)
		atomic.AddInt32(&m.workerCount, 1)
		log.Printf("master.workers : %v\n", m.workers)
		go m.checkMRWorkerAlive(&we) // MRWorker 注册成功，监听心跳
		//args.WId = count
		reply.WId = count

	}
	// 心跳监听机制

	// 是否有已经存在的 IMasterTask 分配给 MRWorker
	err, task := m.getAndBindTask(reply.WId)
	if err != nil {
		log.Println("taskQueue is empty")
		return err
	}
	reply.WTask = task
	return err
}

// 处理 MRWorker 发送过来的心跳
func (m *Master) Heartbeat(args *HeartbeatArgs, reply *HeartbeatReply) error {
	// 向 WorkerElement 的心跳监听队列发送 Pong

	we, err := m.getWorkerElementById(args.WId)
	if err != nil {
		fmt.Printf("%v: %v\n", args.WId, err)
		reply.State = Off
		return err
	}
	log.Printf(" Master receive MRWorker' heartbeat: %v\n", we)
	// 处理心跳带来的消息
	we.Lock()
	reply.State = we.wState
	we.UnLock()

	// 检查任务是否完成
	if atomic.LoadInt32(&m.complete) == 1 {
		go we.sendSign(End)
		reply.WTask = &ExitTask{}
		atomic.AddInt32(&m.workerCount, -1)
		log.Println("done")
		return nil
	}
	if reply.State == On {
		go we.sendSign(Pong)
		// 处理已经完成的任务
		tasks := args.DoneTask
		for i := range tasks {
			err = tasks[i].DealDoneTask(m)
			if err != nil {
				reply.DoneTask = append(reply.DoneTask, tasks[i].TransToWTask())
			}

		}
		// 处理未完成的任务
		tasks = args.ErrTask
		for i := range tasks {
			err = tasks[i].DealErrorTask(m, args.WId)
			if err != nil {
				reply.ErrTask = append(reply.ErrTask, tasks[i].TransToWTask())
			}
		}
		// 如果还有任务就交给worker去做
		err, task := m.getAndBindTask(args.WId)
		if err != nil {
			log.Println("taskQueue is empty")
		}
		reply.WTask = task
	} else {
		go we.sendSign(End)
	}
	log.Println(" heartbeat is ok")
	return nil

}

// 检查 MRWorker 是否alive
func (m *Master) checkMRWorkerAlive(we *WorkerElement) {
	for true {
		if val, err := we.alive.Get(time.Second * 10); err == nil {
			val = val.(HT)
			if val == End { // 说明这个 MRWorker 已经下线
				return
			}
		} else {
			// unregister
			// 超时处理， 把 MRWorker 下线，并且将未完成和出错的任务进行重新分配
			m.workers.Delete(we.id) // 将 MRWorker 从列表删除
			atomic.AddInt32(&m.workerCount, -1)
			log.Printf("this MRWorker has fail : %v", we)
			we.Lock()
			we.wState = Off // 把 MRWorker 下线，并且将任务进行重新分配
			mlist := we.ownMapElements
			rlist := we.ownReduceElements
			we.ownMapElements = list.New()
			we.ownReduceElements = list.New()
			we.UnLock()
			for i := mlist.Front(); i != nil; i = i.Next() {
				me := i.Value.(*MapElement)
				me.Lock()
				if me.state != Complete {
					_ = m.taskQueue.PutNoWait(me.BuildTask())
					me.state = Idle
				}
				me.UnLock()
			}
			for i := rlist.Front(); i != nil; i = i.Next() {
				re := i.Value.(*ReduceElement)
				re.Lock()
				if re.state != Complete {
					_ = m.taskQueue.PutNoWait(re.BuildTask())
					re.state = Idle
				}
				re.UnLock()
			}
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
	if err = mTask.ChangeElementAndTaskState(m, Idle, Progress); err != nil {
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
	m.doneMapTaskCount = 0
	m.doneReduceTaskCount = 0
	m.nMap = len(files)
	m.mapElements = &sync.Map{}
	m.reduceElements = &sync.Map{}
	m.workers = &sync.Map{}
	m.workerCount = 0
	m.complete = 0
	gob.Register(&MapTask{})
	gob.Register(&ReduceTask{})
	gob.Register(&ExitTask{})
	for i, file := range files {
		me := MapElement{Element: Element{MLock{sync.Mutex{}}, Idle, i}, infile: file}
		me.outfile = me.BuildFileNames(m)
		m.mapElements.Store(me.infile, &me)
		_ = m.taskQueue.PutNoWait(me.BuildTask())
	}
}

// MapTask 已经完成
func (m *Master) initReduce() {
	for i := 0; i < m.nReduce; i++ {

		re := ReduceElement{Element: Element{MLock{sync.Mutex{}}, Idle, i}, outfile: ReduceFilePrefix + strconv.Itoa(i)}
		re.infile = re.BuildFileNames(m)
		m.reduceElements.Store(re.outfile, &re)
		_ = m.taskQueue.PutNoWait(re.BuildTask())
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

	if atomic.LoadInt32(&m.complete) == 1 && atomic.LoadInt32(&m.workerCount) == 0 {
		return true
	}

	return false
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
