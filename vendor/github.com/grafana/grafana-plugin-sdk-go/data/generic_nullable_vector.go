package data

type nullablegenVector []*gen

func newNullablegenVector(n int) *nullablegenVector {
	v := nullablegenVector(make([]*gen, n))
	return &v
}

func newNullablegenVectorWithValues(s []*gen) *nullablegenVector {
	v := make([]*gen, len(s))
	copy(v, s)
	return (*nullablegenVector)(&v)
}

func (v *nullablegenVector) Set(idx int, i interface{}) {
	if i == nil {
		(*v)[idx] = nil
		return
	}
	(*v)[idx] = i.(*gen)
}

func (v *nullablegenVector) SetConcrete(idx int, i interface{}) {
	val := i.(gen)
	(*v)[idx] = &val
}

func (v *nullablegenVector) Append(i interface{}) {
	if i == nil {
		*v = append(*v, nil)
		return
	}
	*v = append(*v, i.(*gen))
}

func (v *nullablegenVector) At(i int) interface{} {
	return (*v)[i]
}

func (v *nullablegenVector) CopyAt(i int) interface{} {
	if (*v)[i] == nil {
		var g *gen
		return g
	}
	var g gen
	g = *(*v)[i]
	return &g
}

func (v *nullablegenVector) ConcreteAt(i int) (interface{}, bool) {
	var g gen
	val := (*v)[i]
	if val == nil {
		return g, false
	}
	g = *val
	return g, true
}

func (v *nullablegenVector) PointerAt(i int) interface{} {
	return &(*v)[i]
}

func (v *nullablegenVector) Len() int {
	return len(*v)
}

func (v *nullablegenVector) Type() FieldType {
	return vectorFieldType(v)
}

func (v *nullablegenVector) Extend(i int) {
	*v = append(*v, make([]*gen, i)...)
}

func (v *nullablegenVector) Insert(i int, val interface{}) {
	switch {
	case i < v.Len():
		v.Extend(1)
		copy((*v)[i+1:], (*v)[i:])
		v.Set(i, val)
	case i == v.Len():
		v.Append(val)
	case i > v.Len():
		panic("Invalid index; vector length should be greater or equal to that index")
	}
}

func (v *nullablegenVector) Delete(i int) {
	*v = append((*v)[:i], (*v)[i+1:]...)
}
