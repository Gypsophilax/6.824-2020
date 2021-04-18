package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"sort"
	"strconv"
	"sync/atomic"
)

type ReduceTask struct {
	Number  int
	InFile  []string // 需要读取进行处理的文件
	OutFile string   // 应该输出的文件名
}

func (rt *ReduceTask) ChangeElementAndTaskState(m *Master, oldstate State, newstate State) error {

	if re, ok := m.reduceElements.Load(rt.OutFile); ok {
		re := re.(*ReduceElement)
		re.Lock()
		defer re.UnLock()
		if re.state == oldstate {
			re.state = newstate
			return nil
		}
	}
	_ = m.taskQueue.PutNoWait(rt)
	return errors.New(" error change " + rt.OutFile + " 's state to " + strconv.Itoa(int(newstate)))
}

func (rt *ReduceTask) BindMRWorker(m *Master, workerid int32) error {
	worker, ok := m.workers.Load(workerid)
	re, o := m.reduceElements.Load(rt.OutFile)
	if ok && o {
		worker := worker.(*WorkerElement)
		worker.Lock()
		defer worker.UnLock()
		if worker.wState == Off {
			return nil
		}
		re := re.(*ReduceElement)
		re.Lock()
		defer re.UnLock()
		worker.ownReduceElements.PushBack(re)
		return nil
	}
	return errors.New("bind ReduceTaskElement to MRWorker fail")
}

func (rt *ReduceTask) TransToWTask() IWorkerTask {
	return rt
}

func (rt *ReduceTask) BuildFileNames(m *Master) []string {
	var filenames []string
	for i := 0; i < m.nReduce; i++ {
		filenames = append(filenames, MapFilePrefix+strconv.Itoa(i)+"-"+strconv.Itoa(rt.Number))
	}
	return filenames
}

func (rt *ReduceTask) DealErrorTask(m *Master) error {
	err := rt.ChangeElementAndTaskState(m, Progress, Idle)
	if err != nil {
		return err
	}
	return m.taskQueue.PutNoWait(rt)
}

func (rt *ReduceTask) GetFileName() string {
	return rt.OutFile
}

func (rt *ReduceTask) DealDoneTask(m *Master) error {
	var err error
	if re, ok := m.reduceElements.Load(rt.OutFile); ok {
		re := re.(*ReduceElement)
		re.Lock()
		defer re.UnLock()
		re.state = Complete
		if atomic.AddInt32(&m.doneReduceTaskCount, 1) == int32(m.nReduce) {
			// todo 退出程序的操作
		}

	}
	return err
}

func (rt *ReduceTask) DoTask(w *MRWorker) error {
	// todo println("this is map's task")
	filename := rt.OutFile
	intermediate := make([]KeyValue, 0)
	for i := range rt.InFile {
		inFile, err := os.Open(rt.InFile[i])
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		dec := json.NewDecoder(inFile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		_ = inFile.Close()
	}
	sort.Sort(ByKey(intermediate))
	dir, _ := os.Getwd()
	tempFile, err := ioutil.TempFile(dir, rt.OutFile)
	if err != nil {
		log.Fatalf("cannot create tempfile %v\n", rt.OutFile)
	}

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := w.reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	err = tempFile.Close()
	name := tempFile.Name()
	if err != nil {
		log.Fatal(err)
		_ = os.Remove(name) // ?
	}
	err = os.Rename(name, rt.OutFile)
	fmt.Printf("successfully create map_out file: %v %v\n", name, rt.OutFile)
	return nil
}

func (rt *ReduceTask) TransToMTask() IMasterTask {
	return rt
}

func (rt *ReduceTask) ChangeState(m *Master, state State) error {

	if re, ok := m.reduceElements.Load(rt.OutFile); ok {
		re := re.(*ReduceElement)
		re.Lock()
		defer re.UnLock()
		re.state = state
		return nil
	}
	_ = m.taskQueue.PutNoWait(rt)
	return errors.New(" error change  ReduceElement's state to ")
}
