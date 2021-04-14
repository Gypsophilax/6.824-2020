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
	intermediate := [][]KeyValue{{}, {}, {}, {}, {}, {}, {}, {}, {}, {}}
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

	mt.OutFile = mt.BuildOutputFileNames()
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
func (mt *MapTask) ChangeState(m *Master, state State) error {

	if element := m.mapElements[mt.InFile]; element != nil {
		defer element.lock.Unlock()
		element.lock.Lock()
		element.state = state
		return nil
	}
	return errors.New(" error change " + mt.InFile + " 's state to " + strconv.Itoa(int(state)))
}
func (mt *MapTask) BuildOutputFileNames() []string {
	var filenames []string
	s := Map_File_Prefix + strconv.Itoa(mt.Number) + "-"
	for i := 0; i < 10; i++ {
		filenames = append(filenames, s+strconv.Itoa(i))
	}
	return filenames
}
