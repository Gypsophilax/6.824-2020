package utils

import (
	"sync"
)

// 线程安全的 slice
type Slice struct {
	elements []interface{}
	lock     sync.RWMutex
}

func (l *Slice) Add(element interface{}) {
	l.lock.Lock()
	defer l.lock.Unlock()
	l.elements = append(l.elements, element)
}
func (l *Slice) Get(i int) interface{} {
	l.lock.RLock()
	defer l.lock.RUnlock()
	return l.elements[i]
}
func (l *Slice) Delete(i int) {
	l.lock.Lock()
	defer l.lock.Unlock()
	l.elements = append(l.elements[:i], l.elements[i+1:]...)
}
func NewSlice() *Slice {
	slice := new(Slice)
	slice.lock = sync.RWMutex{}
	return slice
}
