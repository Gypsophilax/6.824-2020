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
	taskQueue *utils.Queue // linkedList
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
	w.doTasker()
	if err != nil {
		log.Fatal(err)
		return
	}

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
	w.taskQueue = utils.New(0)
	w.id = -1
	w.mapf = mapf
	w.reducef = reducef
}

// MRWorker 初次向 Master 注册，可能会返回 Tasker
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
	//  如果有任务分配就放到 taskQueue 中
	if reply.WTasker != nil {
		return w.taskQueue.PutNoWait(reply.WTasker)
	}
	return nil
}

// 循环从队列中获取任务并完成任务
func (w *MRWorker) doTasker() {
	for true {
		if task, err := w.taskQueue.GetNoWait(); err == nil {
			tasker := task.(Tasker)
			fmt.Print(tasker)
			err = tasker.DoTask(w) // todo 如果 error != nil ，应该重试然后向master报告
			if err != nil {
				_ = fmt.Errorf("DoTasker %v", err)
				break
			}
		}
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
