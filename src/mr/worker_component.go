package mr

import "container/list"

type MRWorker struct {
	task      *Task
	taskQueue list.List // linkedList
	id        int32
}
