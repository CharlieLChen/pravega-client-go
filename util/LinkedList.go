package util

import (
	"fmt"
)

type Node struct {
	next  *Node
	value interface{}
}

type FIFOList struct {
	head *Node
	tail *Node
	Size int32
}

func (L *FIFOList) Insert(value interface{}) {
	node := &Node{
		value: value,
	}
	if L.head == nil {
		L.head = &Node{}
		L.head.next = node
		L.tail = node
		L.Size++
		return
	}
	L.tail.next = node
	L.tail = node
	L.Size++
}
func (L *FIFOList) GetLast() interface{} {
	if L.tail != nil {
		return L.tail.value
	}
	return nil
}
func (l *FIFOList) DeleteTo(fn func(a, b interface{}) bool, key interface{}) {
	if l.head == nil {
		return
	}
	node := l.head
	for node.next != nil {
		node = node.next
		if fn(node, key) {
			l.head.next = node.next
		}
	}
}

func (l *FIFOList) ForEach(handler interface{}, fn func(handler, object, c interface{}) error, c interface{}) error {
	if l.head == nil {
		return nil
	}
	node := l.head
	for node.next != nil {
		node = node.next
		err := fn(handler, node, c)
		if err != nil {
			return err
		}
	}
	return nil
}

func (l *FIFOList) Display() {
	list := l.head
	for list != nil {
		fmt.Printf("%+v ->", list.value)
		list = list.next
	}
	fmt.Println()
}
