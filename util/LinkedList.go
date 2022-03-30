package util

import "fmt"

type Node struct {
	next *Node
	key  interface{}
}

type FIFOList struct {
	head *Node
	tail *Node
	Size int32
}

func (L *FIFOList) Insert(key interface{}) {
	node := &Node{
		key: key,
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

func (l *FIFOList) ForEach(handler interface{}, fn func(handler, object interface{}) error) error {
	if l.head == nil {
		return nil
	}
	node := l.head
	for node.next != nil {
		node = node.next
		err := fn(handler, node)
		if err != nil {
			return err
		}
	}
	return nil
}

func (l *FIFOList) Display() {
	list := l.head
	for list != nil {
		fmt.Printf("%+v ->", list.key)
		list = list.next
	}
	fmt.Println()
}
