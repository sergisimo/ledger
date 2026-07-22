package varutils

import (
	"fmt"
	"reflect"
)

// Ptr Returns a pointer from a variable.
func Ptr[T any](t T) *T {
	var v any = t
	if v != nil {
		return &t
	}

	return nil
}

func MustImplement[Iface, V any]() {
	interfaceType := reflect.TypeFor[Iface]()
	if !reflect.TypeFor[V]().Implements(interfaceType) {
		panic(fmt.Sprintf("type: %T does not implement %T", *new(V), new(Iface)))
	}
}
