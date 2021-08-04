package gen

import (
	"reflect"

	"github.com/leanovate/gopter"
)

// Struct generates a given struct type.
// rt has to be the reflect type of the struct, gens contains a map of field generators.
// Note that the result types of the generators in gen have to match the type of the correspoinding
// field in the struct. Also note that only public fields of a struct can be generated
func Struct(rt reflect.Type, gens map[string]gopter.Gen) gopter.Gen {
	if rt.Kind() == reflect.Ptr {
		rt = rt.Elem()
	}
	if rt.Kind() != reflect.Struct {
		return Fail(rt)
	}
	fieldGens := []gopter.Gen{}
	fieldTypes := []reflect.Type{}
	for i := 0; i < rt.NumField(); i++ {
		fieldName := rt.Field(i).Name
		gen := gens[fieldName]
		if gen != nil {
			fieldGens = append(fieldGens, gen)
			fieldTypes = append(fieldTypes, gen(gopter.MinGenParams).ResultType)
		}
	}

	buildStructType := reflect.FuncOf(fieldTypes, []reflect.Type{rt}, false)
	unbuildStructType := reflect.FuncOf([]reflect.Type{rt}, fieldTypes, false)

	buildStructFunc := reflect.MakeFunc(buildStructType, func(args []reflect.Value) []reflect.Value {
		result := reflect.New(rt)
		for i := 0; i < rt.NumField(); i++ {
			if _, ok := gens[rt.Field(i).Name]; !ok {
				continue
			}
			result.Elem().Field(i).Set(args[0])
			args = args[1:]
		}
		return []reflect.Value{result.Elem()}
	})
	unbuildStructFunc := reflect.MakeFunc(unbuildStructType, func(args []reflect.Value) []reflect.Value {
		s := args[0]
		results := []reflect.Value{}
		for i := 0; i < s.NumField(); i++ {
			if _, ok := gens[rt.Field(i).Name]; !ok {
				continue
			}
			results = append(results, s.Field(i))
		}
		return results
	})

	return gopter.DeriveGen(
		buildStructFunc.Interface(),
		unbuildStructFunc.Interface(),
		fieldGens...,
	)
}

// StructPtr generates pointers to a given struct type.
// Note that StructPtr does not generate nil, if you want to include nil in your
// testing you should combine gen.PtrOf with gen.Struct.
// rt has to be the reflect type of the struct, gens contains a map of field generators.
// Note that the result types of the generators in gen have to match the type of the correspoinding
// field in the struct. Also note that only public fields of a struct can be generated
func StructPtr(rt reflect.Type, gens map[string]gopter.Gen) gopter.Gen {
	if rt.Kind() == reflect.Ptr {
		rt = rt.Elem()
	}
	return gopter.DeriveGen(
		func(s interface{}) interface{} {
			sp := reflect.New(rt)
			sp.Elem().Set(reflect.ValueOf(s))
			return sp.Interface()
		},
		func(sp interface{}) interface{} {
			return reflect.ValueOf(sp).Elem().Interface()
		},
		Struct(rt, gens),
	)
}
