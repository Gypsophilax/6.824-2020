package mr

// worker 的状态
const (
	Idle     State = 0
	Progress State = 1
	Complete State = 2
)

type State int

type Task struct {
	Number  int
	InFile  string   // 需要读取进行处理的文件
	State   State    // 任务的状态
	OutFile []string // 应该输出的文件名
}

//func (task *Task) DoTask() {
//	println("this method should be implemented by MapTask and ReduceTask")
//}

type Tasker interface {
	DoTask()
	GetState()
}
