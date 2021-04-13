package mr

// worker 的状态
const (
	Idle               State  = 0
	Progress           State  = 1
	Complete           State  = 2
	Map_File_Prefix    string = "mr-"
	Reduce_File_Prefix string = "mr-out-"
)

type State int

type Task struct {
	Number  int
	InFile  string   // 需要读取进行处理的文件
	State   State    // 任务的状态
	OutFile []string // 应该输出的文件名
}

type Tasker interface {
	DoTask(w *MRWorker) error
	ChangeState(m *Master, state State) error
}
