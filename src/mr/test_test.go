package mr

import "testing"

type EElement struct {
	file  string
	state int // Map 的状态
	id    int
}
type EMapElement struct {
	EElement
}

func TestChange(t *testing.T) {

}
func change(element EMapElement) {
	element.state = 1
}
func TestMakeMaster(t *testing.T) {
	element := EMapElement{EElement{state: 0}}
	println(element.state)
	change(element)

	println(element.state)
}
