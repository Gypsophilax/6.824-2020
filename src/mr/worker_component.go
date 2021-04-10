package mr

type MRWorker struct {
	task      *Task
	taskQueue []Task
	id        int
}
