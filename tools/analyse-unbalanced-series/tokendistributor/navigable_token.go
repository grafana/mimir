package tokendistributor

import (
	"fmt"
)

type navigableTokenInterface interface {
	tokenInfoInterface

	// getNavigableToken returns the navigableToken related to this navigableTokenInterface
	// its goal is to connect tokenInfoInterface with navigableTokenInterface
	getNavigableToken() *navigableToken[*tokenInfo]

	// setNavigableToken sets the navigableToken related to this navigableTokenInterface
	// its goal is to connect tokenInfoInterface with navigableTokenInterface
	setNavigableToken(navigableToken *navigableToken[*tokenInfo])

	// getPrevious returns the navigableTokenInterface preceding this navigableTokenInterface
	getPrevious() navigableTokenInterface

	// getNext returns the navigableTokenInterface succeeding this navigableTokenInterface
	getNext() navigableTokenInterface
}

type navigableToken[T navigableTokenInterface] struct {
	data       T
	prev, next *navigableToken[T]
}

func newNavigableTokenInfo(data *tokenInfo) *navigableToken[*tokenInfo] {
	n := &navigableToken[*tokenInfo]{
		data: data,
	}
	data.setNavigableToken(n)
	return n
}

func newNavigableCandidateTokenInfo(data *candidateTokenInfo) *navigableToken[*candidateTokenInfo] {
	n := &navigableToken[*candidateTokenInfo]{
		data: data,
	}
	//data.setNavigableToken(n)
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

func (e *navigableToken[T]) StringVerbose() string {
	return fmt.Sprintf("%d[%s-%s-%.2f]", e.data.getToken(), e.getData().getOwningInstance().instanceId, e.getData().getOwningInstance().zone.zone, e.getData().getReplicatedOwnership())
}

type CircularList[T navigableTokenInterface] struct {
	head *navigableToken[T]
}

func newCircularList[T navigableTokenInterface]() CircularList[T] {
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
	return c.toString(func(element *navigableToken[T]) string {
		return element.String()
	})
}

func (c *CircularList[T]) StringVerobose() string {
	return c.toString(func(element *navigableToken[T]) string {
		return element.StringVerbose()
	})
}

func (c *CircularList[T]) toString(elementToString func(token *navigableToken[T]) string) string {
	if c.head == nil {
		return "[]"
	}
	last := c.head.prev
	result := fmt.Sprintf("[head=")
	for curr := c.head; curr != last; curr = curr.next {
		result = result + fmt.Sprintf("%v<->", elementToString(curr))
	}
	return result + fmt.Sprintf("%v<->head", last)
}
