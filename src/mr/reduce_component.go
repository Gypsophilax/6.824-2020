package mr

type ReduceTask Task

func (reduceTask *ReduceTask) DoTask() {
	println("this is reduce's task")
}
