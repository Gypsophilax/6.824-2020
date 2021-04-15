package mr

import "time"

// worker 的状态
const (
	Idle     State = 0
	Progress State = 1
	Complete State = 2

	WaitTimeForEmpty time.Duration = time.Second * 3
)
const (
	MapFilePrefix    string = "mr-"
	ReduceFilePrefix string = "mr-out-"
	FileSuffix       string = ".txt"
)

const (
	On  WorkerState = 0
	Off WorkerState = 1
)

const (
	Pong HT = 0
	End  HT = 1
)

type State int
type WorkerState int
type HT int
type Task struct {
	Number  int
	InFile  string   // 需要读取进行处理的文件
	OutFile []string // 应该输出的文件名
}

// Master Task 数据结构
type IMasterTask interface {
	ChangeElementAndTaskState(m *Master, state State) error
	BindMRWorker(m *Master, workerid int32) error
	TransToWTask() IWorkerTask
	BuildOutputFileNames() []string
}

// MRWorker Task 数据结构
type IWorkerTask interface {
	DoTask(w *MRWorker) error
	TransToMTask() IMasterTask
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }
