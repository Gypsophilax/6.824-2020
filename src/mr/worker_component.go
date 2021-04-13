package mr

import (
	"errors"
	"strconv"
)

type WorkerTask struct {
	Task
}

func (wt *WorkerTask) DoTask(w *MRWorker) error {
	println("this is map's task")
	wt.State = Complete
	println("worker do %v", wt.State)

	return nil
}
func (wt *WorkerTask) ChangeState(m *Master, state State) error {

	if element := m.reduceElements[wt.InFile]; element != nil {
		defer element.lock.Unlock()
		element.lock.Lock()
		element.state = state
		return nil
	}
	return errors.New(" error change " + wt.InFile + " 's state to " + strconv.Itoa(int(state)))
}
