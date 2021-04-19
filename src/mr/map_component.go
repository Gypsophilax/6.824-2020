package mr

import (
	"container/list"
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

type MapTask struct {
	Number  int
	InFile  string   // 需要读取进行处理的文件
	OutFile []string // 应该输出的文件名
}

func (mt *MapTask) DoTask(w *MRWorker) error {
	log.Printf("worker deals MapTask :%v", *mt)
	filename := mt.InFile
	intermediate := make([][]KeyValue, len(mt.OutFile))
	inFile, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(inFile)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	_ = inFile.Close()
	kva := w.mapf(filename, string(content))
	sort.Sort(ByKey(kva))
	// 1. 根据key进行划分
	for i := range kva {
		kv := kva[i]
		index := ihash(kv.Key) % 10
		intermediate[index] = append(intermediate[index], kv)
	}
	for i := range intermediate {
		// 2. 转成 json 写入临时文件
		dir, _ := os.Getwd()
		tempFile, err := ioutil.TempFile(dir, mt.OutFile[i])
		if err != nil {
			log.Fatalf("cannot create tempfile %v\n", mt.OutFile[i])
		}
		encoder := json.NewEncoder(tempFile)
		for j := range intermediate[i] {
			if err := encoder.Encode(intermediate[i][j]); err != nil {
				log.Fatalf(" transform %v to json fail\n", intermediate[i][j]) // todo 编码失败文件重新处理
			}
		}
		err = tempFile.Close()
		name := tempFile.Name()
		if err != nil {
			log.Fatal(err)
			_ = os.Remove(name) // ?
		}
		// 3. 重命名临时文件
		err = os.Rename(name, mt.OutFile[i])
		fmt.Printf("successfully create map_out file: %v %v\n", name, mt.OutFile[i])
	}

	return nil
}

// 改变 MapElement 的状态
func (mt *MapTask) ChangeElementAndTaskState(m *Master, oldstate State, newstate State) error {

	if me, ok := m.mapElements.Load(mt.InFile); ok {
		me := me.(*MapElement)
		me.Lock()
		defer me.UnLock()
		if me.state == oldstate {
			me.state = newstate
			return nil
		}
	}
	_ = m.taskQueue.PutNoWait(mt)
	return errors.New(" error change " + mt.InFile + " 's state to " + strconv.Itoa(int(newstate)))
}

// 将 MapElement 添加到 taskQueue
func (mt *MapTask) DealErrorTask(m *Master, workerid int32) error {
	log.Printf("failure mapTask :%v\n", *mt)
	err := mt.ChangeElementAndTaskState(m, Progress, Idle)
	worker, ok := m.workers.Load(workerid)
	me, o := m.mapElements.Load(mt.InFile)
	// 解绑
	if ok && o {
		worker := worker.(*WorkerElement)
		worker.Lock()
		defer worker.UnLock()
		me := me.(*MapElement)
		me.Lock()
		defer me.UnLock()
		var del *list.Element
		for del = worker.ownMapElements.Front(); del != nil; del = del.Next() {
			if del.Value.(*MapElement).id == me.id {
				break
			}
		}
		worker.ownMapElements.Remove(del)
	}
	if err != nil {
		return err
	}
	return m.taskQueue.PutNoWait(mt)
}

func (mt *MapTask) GetFileName() string {
	return mt.InFile
}

func (mt *MapTask) BuildFileNames(m *Master) []string {
	var filenames []string
	s := MapFilePrefix + strconv.Itoa(mt.Number) + "-"
	for i := 0; i < m.nReduce; i++ {
		filenames = append(filenames, s+strconv.Itoa(i)+FileSuffix)
	}
	return filenames
}

// 将 IMasterTask 对应的 TaskElement 和 MRWorker进行绑定
func (mt *MapTask) BindMRWorker(m *Master, workerid int32) error {
	worker, ok := m.workers.Load(workerid)
	me, o := m.mapElements.Load(mt.InFile)
	if ok && o {
		worker := worker.(*WorkerElement)
		worker.Lock()
		defer worker.UnLock()
		if worker.wState == Off {
			return nil
		}
		me := me.(*MapElement)
		me.Lock()
		defer me.UnLock()
		worker.ownMapElements.PushBack(me)
		return nil
	}
	return errors.New("bind MapTaskElement to MRWorker fail")
}
func (mt *MapTask) TransToWTask() IWorkerTask {
	return mt
}
func (mt *MapTask) TransToMTask() IMasterTask {
	return mt
}

// MapTask 被完成的时候进行的操作
func (mt *MapTask) DealDoneTask(m *Master) error {
	log.Printf("successful mapTask :%v\n", *mt)
	var err error
	if me, ok := m.mapElements.Load(mt.InFile); ok {
		me := me.(*MapElement)
		// todo 创建 ReduceElement
		me.Lock()
		defer me.UnLock()
		me.state = Complete
		if atomic.AddInt32(&m.doneMapTaskCount, 1) == int32(m.nMap) {
			// todo 生成 ReduceTask
			log.Println("create reduceTask")
			m.initReduce()
		}

	}
	return err
}
