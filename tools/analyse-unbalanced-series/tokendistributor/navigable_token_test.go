package tokendistributor

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCircularList_InsertFirst(t *testing.T) {
	tokenInfo := newTokenInfo(newInstanceInfo("instance-1", newZoneInfo("zone-1"), 4), 1)
	firstElement := newNavigableToken(tokenInfo)
	circularList := newCircularListNavigableTokenInterface[navigableTokenInterface]()

	head := circularList.insertFirst(firstElement)
	require.Equal(t, head, firstElement)

	secondElement := newNavigableToken(newCandidateTokenInfo(newInstanceInfo("instance-2", newZoneInfo("zone-2"), 4), 2, tokenInfo))
	head = circularList.insertFirst(secondElement)
	require.Equal(t, head, secondElement)

	thirdElement := newNavigableToken(newTokenInfo(newInstanceInfo("instance-3", newZoneInfo("zone-3"), 4), 3))
	head = circularList.insertFirst(thirdElement)
	require.Equal(t, head, thirdElement)

	require.Equal(t, head.prev, firstElement)
	require.Equal(t, head.next, secondElement)
	require.Equal(t, head.next.next, firstElement)
	require.Equal(t, head.next.prev, thirdElement)
	require.Equal(t, head.prev.next, thirdElement)
	require.Equal(t, head.prev.prev, secondElement)
}

func TestCircularList_InsertLast(t *testing.T) {
	tokenInfo := newTokenInfo(newInstanceInfo("instance-1", newZoneInfo("zone-1"), 4), 1)
	firstElement := newNavigableToken(tokenInfo)
	circularList := newCircularListNavigableTokenInterface[navigableTokenInterface]()

	head := circularList.insertLast(firstElement)
	require.Equal(t, head, firstElement)

	secondElement := newNavigableToken(newCandidateTokenInfo(newInstanceInfo("instance-2", newZoneInfo("zone-2"), 4), 2, tokenInfo))
	head = circularList.insertLast(secondElement)
	require.Equal(t, head, firstElement)

	thirdElement := newNavigableToken(newTokenInfo(newInstanceInfo("instance-3", newZoneInfo("zone-3"), 4), 3))
	head = circularList.insertLast(thirdElement)
	require.Equal(t, head, firstElement)

	require.Equal(t, head.prev, thirdElement)
	require.Equal(t, head.next, secondElement)
	require.Equal(t, head.next.next, thirdElement)
	require.Equal(t, head.next.prev, firstElement)
	require.Equal(t, head.prev.next, firstElement)
	require.Equal(t, head.prev.prev, secondElement)
}

func TestCircularList_Remove(t *testing.T) {
	tokenInfo := newTokenInfo(newInstanceInfo("instance-1", newZoneInfo("zone-1"), 4), 1)
	firstElement := newNavigableToken(tokenInfo)
	secondElement := newNavigableToken(newCandidateTokenInfo(newInstanceInfo("instance-2", newZoneInfo("zone-2"), 4), 2, tokenInfo))
	thirdElement := newNavigableToken(newTokenInfo(newInstanceInfo("instance-3", newZoneInfo("zone-3"), 4), 3))
	circularList := newCircularListNavigableTokenInterface[navigableTokenInterface]()

	circularList.insertLast(firstElement)
	circularList.insertLast(secondElement)
	circularList.insertLast(thirdElement)

	head := circularList.remove(secondElement)
	require.Equal(t, head, firstElement)
	require.Equal(t, head.next, thirdElement)
	require.Equal(t, head.prev, thirdElement)
	require.Nil(t, secondElement.prev)
	require.Nil(t, secondElement.next)

	head = circularList.remove(firstElement)
	require.Equal(t, head, thirdElement)
	require.Equal(t, head.next, thirdElement)
	require.Equal(t, head.prev, thirdElement)
	require.Nil(t, firstElement.prev)
	require.Nil(t, firstElement.next)

	head = circularList.remove(thirdElement)
	require.Nil(t, head)
	require.Nil(t, firstElement.prev)
	require.Nil(t, firstElement.next)
}

func TestNavigableToken_InsertBefore(t *testing.T) {
	tokenInfo := newTokenInfo(newInstanceInfo("instance-1", newZoneInfo("zone-1"), 4), 1)
	firstElement := newNavigableToken(tokenInfo)
	secondElement := newNavigableToken(newCandidateTokenInfo(newInstanceInfo("instance-2", newZoneInfo("zone-2"), 4), 2, tokenInfo))
	secondElement.prev = secondElement
	secondElement.next = secondElement

	firstElement.insertBefore(secondElement)
	require.NotNil(t, firstElement.next)
	require.NotNil(t, firstElement.prev)
	require.NotNil(t, secondElement.next)
	require.NotNil(t, secondElement.prev)
	require.Equal(t, firstElement.next, secondElement)
	require.Equal(t, secondElement.prev, firstElement)
}

func TestNavigableToken_GetNavigableToken(t *testing.T) {
	firstElement := newNavigableTokenInfo(newTokenInfo(newInstanceInfo("instance-1", newZoneInfo("zone-1"), 4), 1))
	secondElemet := newNavigableTokenInfo(newTokenInfo(newInstanceInfo("instance-2", newZoneInfo("zone-2"), 4), 2))
	circularList := newCircularListNavigableTokenInterface[*tokenInfo]()
	circularList.insertLast(firstElement)
	circularList.insertLast(secondElemet)
	head := circularList.head
	first := head.getData()
	require.Equal(t, first.getNavigableToken(), head)
	tail := head.prev
	last := tail.getData()
	require.Equal(t, last.getNavigableToken(), tail)
}
