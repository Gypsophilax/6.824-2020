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
)

type MapTask struct {
	Task
}

func (mt *MapTask) DoTask(w *MRWorker) error {
	filename := mt.InFile
	intermediate := make([][]KeyValue, 10)
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
func (mt *MapTask) ChangeElementAndTaskState(m *Master, state State) error {

	if me, ok := m.mapElements.Load(mt.InFile); ok {
		me := me.(*MapElement)
		me.Lock()
		defer me.UnLock()
		me.state = state
		return nil
	}
	_ = m.taskQueue.PutNoWait(mt)
	return errors.New(" error change " + mt.InFile + " 's state to " + strconv.Itoa(int(state)))
}
func (mt *MapTask) BuildOutputFileNames() []string {
	var filenames []string
	s := MapFilePrefix + strconv.Itoa(mt.Number) + "-"
	for i := 0; i < 10; i++ {
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
