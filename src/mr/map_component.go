package mr

import (
	"errors"
	"io/ioutil"
	"log"
	"os"
	"strconv"
)

type MapTask struct {
	Task
}

func (mt *MapTask) DoTask(w *MRWorker) error {
	filename := mt.InFile
	var intermediate [][]KeyValue
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
	for i := range kva {

	}
	// todo 创建临时文件，并根据 ihash 方法进行分组
	// 生成临时文件 ioutil.TempFile()
	// os.Rename()
	return nil
}
func (mt *MapTask) ChangeState(m *Master, state State) error {

	if element := m.mapElements[mt.InFile]; element != nil {
		defer element.lock.Unlock()
		element.lock.Lock()
		element.state = state
		return nil
	}
	return errors.New(" error change " + mt.InFile + " 's state to " + strconv.Itoa(int(state)))
}
