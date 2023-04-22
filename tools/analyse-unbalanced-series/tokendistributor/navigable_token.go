package tokendistributor

import (
	"fmt"
)

type navigableTokenInterface interface {
	tokenInfoInterface

	// getNavigableToken returns the navigableToken related to this navigableTokenInterface
	// its goal is to connect tokenInfoInterface with navigableTokenInterface
	getNavigableToken() *navigableToken[navigableTokenInterface]

	// setNavigableToken sets the navigableToken related to this navigableTokenInterface
	// its goal is to connect tokenInfoInterface with navigableTokenInterface
	setNavigableToken(navigableToken *navigableToken[navigableTokenInterface])

	// getPrevious returns the navigableTokenInterface preceding this navigableTokenInterface
	getPrevious() navigableTokenInterface
}

type navigableToken[T navigableTokenInterface] struct {
	data       T
	prev, next *navigableToken[T]
}

func newNavigableToken(data navigableTokenInterface) *navigableToken[navigableTokenInterface] {
	n := &navigableToken[navigableTokenInterface]{
		data: data,
	}
	data.setNavigableToken(n)
	return n
}

func (e *navigableToken[T]) insertBefore(element *navigableToken[T]) {
	e.prev = element.prev
	e.next = element
	e.next.prev = e
	if e.prev != nil {
		e.prev.next = e
	}
}

func (e *navigableToken[T]) getData() T {
	return e.data
}

func (e *navigableToken[T]) getPrev() T {
	return e.prev.data
}

func (e *navigableToken[T]) getNext() T {
	return e.next.data
}

func (e *navigableToken[T]) String() string {
	return fmt.Sprintf("%d", e.data.getToken())
}

type CircularList[T navigableTokenInterface] struct {
	head *navigableToken[T]
}

func newCircularListNavigableTokenInterface[T navigableTokenInterface]() CircularList[T] {
	return CircularList[T]{
		head: nil,
	}
}

func (c *CircularList[T]) insertFirst(newElement *navigableToken[T]) *navigableToken[T] {
	if c.head == nil {
		newElement.prev = newElement
		newElement.next = newElement
	} else {
		newElement.insertBefore(c.head)
	}
	c.head = newElement
	return c.head
}

func (c *CircularList[T]) insertLast(newElement *navigableToken[T]) *navigableToken[T] {
	if c.head == nil {
		newElement.prev = newElement
		newElement.next = newElement
		c.head = newElement
	} else {
		c.head.prev.next = newElement
		newElement.prev = c.head.prev
		newElement.next = c.head
		c.head.prev = newElement
	}
	return c.head
}

func (c *CircularList[T]) remove(element *navigableToken[T]) *navigableToken[T] {
	if element == c.head {
		// if list contains only one element, it will become empty
		if c.head.next == c.head || c.head.prev == c.head {
			c.head.next = nil
			c.head.prev = nil
			c.head = nil
			return c.head
		}
		c.head = element.next
	}
	next := element.next
	next.prev = element.prev
	element.prev.next = next
	element.prev = nil
	element.next = nil
	return c.head
}

func (c *CircularList[T]) String() string {
	if c.head == nil {
		return "[]"
	}
	last := c.head.prev
	result := fmt.Sprintf("[head=")
	for curr := c.head; curr != last; curr = curr.next {
		result = result + fmt.Sprintf("%v<->", curr)
	}
	return result + fmt.Sprintf("%v<->head", last)
}
