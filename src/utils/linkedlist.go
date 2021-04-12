package utils

import (
	"container/list"
	"sync"
)

type LinkedList struct {
	list list.List
	lock sync.RWMutex
}

func (linkedList *LinkedList) Put(element interface{}) {
	defer linkedList.lock.Unlock()
	linkedList.lock.Lock()
	linkedList.Put(element)
}
