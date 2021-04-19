package mr

import (
	"os"
	"time"
)

// worker 的状态
const (
	Idle     State = 0
	Progress State = 1
	Complete State = 2

	WaitTimeForEmpty time.Duration = time.Second
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

// Master Task 数据结构
type IMasterTask interface {
	ChangeElementAndTaskState(m *Master, oldstate State, newstate State) error
	BindMRWorker(m *Master, workerid int32) error
	TransToWTask() IWorkerTask
	BuildFileNames(m *Master) []string
	DealErrorTask(m *Master, workerid int32) error
	GetFileName() string
	DealDoneTask(m *Master) error
}

// MRWorker Task 数据结构
type IWorkerTask interface {
	DoTask(w *MRWorker) error
	TransToMTask() IMasterTask
	GetFileName() string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type ExitTask struct {
}

func (e *ExitTask) GetFileName() string {
	panic("implement me")
}

func (e *ExitTask) DoTask(w *MRWorker) error {
	// todo 完成任务 退出
	os.Exit(0)
	return nil
}

func (e *ExitTask) TransToMTask() IMasterTask {
	panic("implement me")
}
