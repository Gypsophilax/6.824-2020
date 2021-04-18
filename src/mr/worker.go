package mr

import (
	"../utils"
	"encoding/gob"
	"fmt"
	"sync/atomic"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type MRWorker struct {
	todoTask  *utils.Queue // 未处理的 IWorkerTask
	doingTask *utils.Queue // MRWorker 负责的 IWorkerTask
	doneTask  *utils.Queue
	errTask   *utils.Queue
	id        int32
	mapf      func(string, string) []KeyValue
	reducef   func(string, []string) string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	w := new(MRWorker)
	w.init(mapf, reducef)
	err := w.Register()
	w.doMTask()
	if err != nil {
		log.Fatal(err)
		return
	}
	w.sendHeartbeat()

}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

// 初始化 MRWorker
func (w *MRWorker) init(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	gob.Register(&MapTask{})
	w.todoTask = utils.New(0)
	w.doingTask = utils.New(0)
	w.doneTask = utils.New(0)
	w.errTask = utils.New(0)
	w.id = -1
	w.mapf = mapf
	w.reducef = reducef
}

// MRWorker 初次向 Master 注册，可能会返回 IMasterTask
func (w *MRWorker) Register() error {
	args := RegisterArgs{WId: w.id}
	reply := RegisterReply{}
	for args.WId = atomic.LoadInt32(&w.id); args.WId < 0; args.WId = atomic.LoadInt32(&w.id) {
		if call("Master.Register", &args, &reply) {
			atomic.CompareAndSwapInt32(&w.id, -1, reply.WId)
		} else {
			time.Sleep(time.Second)
		}

	}
	//  如果有任务分配就放到 todoTask 中
	if reply.WTask != nil {
		_ = w.todoTask.PutNoWait(reply.WTask)
	}
	return nil
}

// 循环从队列中获取任务并完成任务
func (w *MRWorker) doMTask() {
	//for true {
	if task, err := w.todoTask.GetNoWait(); err == nil {
		wTask := task.(IWorkerTask)
		_ = w.doingTask.PutNoWait(wTask)
		err = wTask.DoTask(w)
		if err != nil { // 如果 error != nil ，应该重试然后向master报告
			_ = fmt.Errorf("DoTasker %v", err)
			_ = w.errTask.PutNoWait(task)
			//break
		} else {
			// 向 Master 报告任务完成
			_ = w.doneTask.PutNoWait(task)
		}
	} else {
		time.Sleep(WaitTimeForEmpty)
	}
	//}
}

// 向 Master 发送心跳
func (w *MRWorker) sendHeartbeat() {
	for true {
		log.Println(" MRWorker send Master heartbeat")
		args := HeartbeatArgs{WId: w.id}
		doneTask := w.doneTask.GetAll()
		errTask := w.errTask.GetAll()
		for i := range doneTask {
			dtask := doneTask[i].(IMasterTask)
			args.DoneTask = append(args.DoneTask, dtask)
		}
		for i := range errTask {
			etask := errTask[i].(IMasterTask)
			args.ErrTask = append(args.ErrTask, etask)
		}
		reply := HeartbeatReply{}
		call("Master.Heartbeat", &args, &reply)
		if reply.State == Off {
			// todo 说明 MRWorker 已经被 Master 认定为下线
			//_ = w.Register()
			break
		}

		dtask := reply.DoneTask
		for i := range dtask {
			w.doneTask.PutNoWait(dtask[i])
		}
		etask := reply.ErrTask
		for i := range etask {
			w.errTask.PutNoWait(etask[i])
		}
		//  如果有任务分配就放到 todoTask 中
		if reply.WTask != nil {
			_ = w.todoTask.PutNoWait(reply.WTask)
		}
		time.Sleep(time.Second * 3)
	}
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
