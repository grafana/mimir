package eventlogger

import "sync"

// TODO: remove this if Go ever introduces sync.Map with generics

// graphMap implements a type-safe synchronized map[PipelineID]*linkedNode
type graphMap struct {
	m sync.Map
}

// Range calls sync.Map.Range
func (g *graphMap) Range(f func(key PipelineID, value *linkedNode) bool) {
	g.m.Range(func(key, value interface{}) bool {
		return f(key.(PipelineID), value.(*linkedNode))
	})
}

// Store calls sync.Map.Store
func (g *graphMap) Store(id PipelineID, root *linkedNode) {
	g.m.Store(id, root)
}

// Delete calls sync.Map.Delete
func (g *graphMap) Delete(id PipelineID) {
	g.m.Delete(id)
}
