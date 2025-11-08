package list

import "container/list"

// Node 包装原始的list，使其支持泛型
type Node[T any] struct {
	inner *list.Element
}

// Value 方法返回具体类型 T 的值
// 这是关键：在这里进行类型断言
func (e *Node[T]) Value() T {
	// 我们确信存入的值一定是 T 类型，所以可以断言
	return e.inner.Value.(T)
}

// Next 返回下一个泛型元素
func (e *Node[T]) Next() *Node[T] {
	next := e.inner.Next()
	if next == nil {
		return nil
	}
	return &Node[T]{inner: next}
}

// Prev 返回上一个元素
func (e *Node[T]) Prev() *Node[T] {
	prev := e.inner.Prev()
	if prev == nil {
		return nil
	}
	return &Node[T]{inner: prev}
}

// List 是对 list.List 的泛型包装
type List[T any] struct {
	// 内部持有非泛型的 list.List
	inner *list.List
}

func New[T any]() *List[T] {
	return &List[T]{
		inner: list.New(),
	}
}

// PushBack 在列表尾部添加一个 T 类型的元素
func (l *List[T]) PushBack(value T) {
	l.inner.PushBack(value)
}

// PushFront 在列表头部添加一个 T 类型的元素
func (l *List[T]) PushFront(value T) {
	l.inner.PushFront(value)
}

// Begin 返回开始的迭代器
func (l *List[T]) Begin() *Node[T] {
	back := l.inner.Back()
	if back == nil {
		panic("list is empty")
	}
	return &Node[T]{inner: back}
}

// Back 返回尾部的泛型元素
func (l *List[T]) Back() T {
	back := l.inner.Back()
	if back == nil {
		panic("list is empty")
	}
	n := &Node[T]{inner: back}
	return n.Value()
}

// Front 返回头部的泛型元素
func (l *List[T]) Front() T {
	front := l.inner.Front()
	if front == nil {
		panic("list is empty")
	}
	n := &Node[T]{inner: front}
	return n.Value()
}

// PopBack 移除并返回尾部元素的值
func (l *List[T]) PopBack() T {
	back := l.inner.Back()
	if back == nil {
		panic("list is empty")
	}

	// 先移除，再断言
	val := l.inner.Remove(back)
	return val.(T)
}

// PopFront 移除并返回头部元素的值
// 这是另一个关键点：取出时进行类型断言
func (l *List[T]) PopFront() T {
	front := l.inner.Front()
	if front == nil {
		panic("list is empty")
	}

	// 先移除，再断言
	val := l.inner.Remove(front)
	return val.(T)
}

// Len 返回列表的长度 (直接传递调用)
func (l *List[T]) Len() int {
	return l.inner.Len()
}

// Empty 判断列表是否为空
func (l *List[T]) Empty() bool {
	return l.inner.Len() == 0
}
