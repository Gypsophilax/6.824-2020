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

	if me, ok := m.reduceElements.Load(rt.InFile); ok {
		me := me.(*MapElement)
		me.Lock()
		defer me.UnLock()
		me.state = state
		return nil
	}
	_ = m.taskQueue.PutNoWait(rt)
	return errors.New(" error change " + rt.InFile + " 's state to " + strconv.Itoa(int(state)))
}
