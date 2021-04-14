package utils

import (
	"../mr"
	"sync"
)

// 线程安全的 slice
type Slice struct {
	elements []interface{}
	mr.MLock
}

func (s *Slice) Add(element interface{}) {
	s.Lock()
	defer s.UnLock()
	s.elements = append(s.elements, element)
}
func (s *Slice) Get(i int) interface{} {
	s.Lock()
	defer s.UnLock()
	return s.elements[i]
}
func (s *Slice) Delete(i int) {
	s.Lock()
	defer s.UnLock()
	s.elements = append(s.elements[:i], s.elements[i+1:]...)
}
func (s *Slice) Clone() []interface{} {
	s.Lock()
	defer s.UnLock()
	var cElements []interface{}
	copy(s.elements, cElements)
	return cElements
}
func NewSlice() *Slice {
	slice := new(Slice)
	slice.MLock = mr.MLock{Mutex: sync.Mutex{}}
	return slice
}
