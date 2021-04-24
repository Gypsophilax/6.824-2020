package raft

import (
	"fmt"
	"testing"
	"time"
)

func TestTimer(t *testing.T) {

	timer := time.NewTimer(time.Second * 10)
	time.Sleep(time.Second * 8)
	fmt.Println("8s pass")
	//timer.Reset(time.Second*6)
	go func() {
		for true {
			fmt.Println("one second pass")
			time.Sleep(time.Second)
		}
	}()
	<-timer.C
	fmt.Println("time out")

}
