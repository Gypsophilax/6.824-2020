package mr

import (
	"errors"
	"strconv"
)

type ReduceTask struct {
	Task
}

func (rt *ReduceTask) DoTask(w *MRWorker) error {
	println("this is map's task")
	rt.State = Complete
	println("worker do %v", rt.State)

	return nil
}
func (rt *ReduceTask) ChangeState(m *Master, state State) error {

	if element := m.reduceElements[rt.InFile]; element != nil {
		defer element.lock.Unlock()
		element.lock.Lock()
		element.state = state
		return nil
	}
	return errors.New(" error change " + rt.InFile + " 's state to " + strconv.Itoa(int(state)))
}
