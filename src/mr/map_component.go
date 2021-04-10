package mr

type MapTask struct {
	Task
}

func (mapTask *MapTask) DoTask() {
	println("this is map's task")
	mapTask.State = Complete
	println("worker do %v", mapTask.State)
}
func (mapTask *MapTask) GetState() {
	println("master")
	println(mapTask.State)
}
