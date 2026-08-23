// Package sets offers an implementation of a set (collection without duplicate values) and some
// utilities/functions to work with sets
package sets

import (
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/sergisimo/ledger/internal/platform/types"

	"github.com/sergisimo/ledger/internal/platform/utils/sliceutils"
)

type (
	Set[T comparable] interface {
		Values() []T
		Add(elems ...T)
		Delete(elems ...T)
		Has(elems ...T) bool
		Intersect(other ...Set[T]) Set[T]
		Diff(other ...Set[T]) Set[T]
		SymmetricDiff(other ...Set[T]) Set[T]
		Equal(other ...Set[T]) bool
		Len() int
	}

	config[T any] struct {
		keepInsertIdx bool
		initialElems  []T
	}

	Option[T any] func(c *config[T])

	set[T comparable]     map[T]struct{}
	Ordered[T comparable] struct {
		set[T]

		valByIdx map[int]T
	}

	sortable interface {
		cmp.Ordered
		comparable
	}
)

var errUnmarshalOrdered = errors.New("could not unmarshal to ordered set")

// With allows to fill the set with initial values.
func With[T any](elems ...T) Option[T] {
	return func(c *config[T]) {
		c.initialElems = elems
	}
}

// KeepOrder allows to keep the index of the inserted elements,
// so Values() will return determinate results based on the insertion order of the set.
func KeepOrder[T comparable](c *config[T]) {
	c.keepInsertIdx = true
}

// Unordered doesn't keep the index of the inserted elements,
// so Values() will return indeterminate results, without taking into account the insertion order of the set.
func Unordered[T comparable](c *config[T]) {
	c.keepInsertIdx = false
}

func defaultOpts[T comparable]() []Option[T] {
	return []Option[T]{
		With([]T{}...),
		Unordered[T],
	}
}

func New[T comparable](opts ...Option[T]) Set[T] {
	cfg := new(config[T])
	for _, opt := range append(defaultOpts[T](), opts...) {
		opt(cfg)
	}

	var set Set[T]
	if !cfg.keepInsertIdx {
		set = newSet[T]()
	} else {
		set = newOrdered[T]()
	}

	set.Add(cfg.initialElems...)

	return set
}

func SortedValues[T sortable](s Set[T]) []T {
	res := s.Values()
	slices.Sort(res)

	return res
}

func ToProto[P ~int32, T types.Enum](set Set[T], protoEnumValMap map[string]int32, protoEnumPrefix string) []P {
	_, isOrdered := set.(*Ordered[T])
	if !isOrdered {
		set = New(With(set.Values()...), KeepOrder)
	}

	prefix := strings.TrimSuffix(protoEnumPrefix, "_")

	return sliceutils.Map(set.Values(), func(v T) P {
		return P(protoEnumValMap[fmt.Sprintf("%s_%s", prefix, v.String())])
	})
}

func FromProto[T types.Enum, P ~int32](vals []P, protoEnumNameMap map[int32]string, protoEnumPrefix string) Set[T] {
	prefix := strings.TrimSuffix(protoEnumPrefix, "_") + "_"

	return New(
		With(sliceutils.Map(vals, func(v P) T {
			return T(strings.TrimPrefix(protoEnumNameMap[int32(v)], prefix))
		})...),
		KeepOrder,
	)
}

func ToRestDTO[T sortable](set Set[T]) *Ordered[T] {
	if set == nil {
		return nil
	}

	v, isOrdered := set.(*Ordered[T])
	if isOrdered {
		return v
	}

	genSet := New(With(SortedValues(set)...), KeepOrder)
	if ordered, ok := genSet.(*Ordered[T]); ok {
		return ordered
	}
	return nil
}

func (o *Ordered[T]) Values() []T {
	if o == nil {
		return nil
	}

	res := make([]T, o.Len())
	for i := range o.Len() {
		res[i] = o.valByIdx[i]
	}

	return res
}

func (o *Ordered[T]) Add(elems ...T) {
	for _, elem := range elems {
		if !o.Has(elem) {
			o.valByIdx[o.Len()] = elem
			o.set[elem] = struct{}{}
		}
	}
}

func (o *Ordered[T]) findIdx(elem T) int {
	for idx, v := range o.valByIdx {
		if v == elem {
			return idx
		}
	}

	return -1
}

func (o *Ordered[T]) Delete(elems ...T) {
	for _, elem := range elems {
		if o.Has(elem) {
			idx := o.findIdx(elem)
			if idx > -1 {
				delete(o.valByIdx, idx)
			}

			for i := idx + 1; i < o.Len(); i++ {
				o.valByIdx[i-1] = o.valByIdx[i]
			}

			delete(o.set, elem)
		}
	}
}

func (o *Ordered[T]) Intersect(other ...Set[T]) Set[T] {
	return intersect(o, other, KeepOrder)
}

func (o *Ordered[T]) Diff(other ...Set[T]) Set[T] {
	return diff(o, other, KeepOrder)
}

func (o *Ordered[T]) SymmetricDiff(other ...Set[T]) Set[T] {
	return symmetricDiff(o, other, KeepOrder)
}

func (o Ordered[T]) MarshalJSON() ([]byte, error) {
	return json.Marshal(o.Values())
}

func (o Ordered[T]) MarshalJSONAPIField() ([]byte, error) {
	return o.MarshalJSON()
}

func (o *Ordered[T]) UnmarshalJSONAPIField(data []byte) error {
	return o.UnmarshalJSON(data)
}

func (o *Ordered[T]) UnmarshalJSON(data []byte) error {
	if string(data) == "null" {
		return nil
	}

	vals := []T{}
	err := json.Unmarshal(data, &vals)
	if err != nil {
		return err
	}

	genSet := New(With(vals...), KeepOrder)
	ordered, ok := genSet.(*Ordered[T])
	if !ok {
		return errUnmarshalOrdered
	}
	*o = *ordered

	return nil
}

func (s set[T]) Values() []T {
	return slices.Collect(maps.Keys(s))
}

func (s set[T]) Add(elems ...T) {
	for _, elem := range elems {
		if !s.Has(elem) {
			s[elem] = struct{}{}
		}
	}
}

func (s set[T]) Delete(elems ...T) {
	for _, elem := range elems {
		delete(s, elem)
	}
}

func (s set[T]) Has(elems ...T) bool {
	for _, elem := range elems {
		if _, exists := s[elem]; !exists {
			return false
		}
	}

	return true
}

func diff[T comparable](s Set[T], other []Set[T], opts ...Option[T]) Set[T] {
	if len(other) == 0 {
		return s
	}
	res := New(opts...)
	for _, elem := range s.Values() {
		existsInAny := false
		for _, next := range other {
			if next.Has(elem) {
				existsInAny = true
				break
			}
		}
		if !existsInAny {
			res.Add(elem)
		}
	}

	return res
}

func intersect[T comparable](s Set[T], other []Set[T], opts ...Option[T]) Set[T] {
	if len(other) == 0 {
		return New[T]()
	}
	res := New(opts...)
	for _, elem := range s.Values() {
		existsInAll := true
		for _, next := range other {
			if !next.Has(elem) {
				existsInAll = false
				break
			}
		}
		if existsInAll {
			res.Add(elem)
		}
	}

	return res
}

func symmetricDiff[T comparable](s Set[T], other []Set[T], opts ...Option[T]) Set[T] {
	allSets := append([]Set[T]{s}, other...)
	elemCount := make(map[T]int)
	for _, set := range allSets {
		for _, elem := range set.Values() {
			elemCount[elem]++
		}
	}
	res := New(opts...)
	for elem, count := range elemCount {
		if count%2 == 1 {
			res.Add(elem)
		}
	}
	return res
}

func (s set[T]) Intersect(other ...Set[T]) Set[T] {
	return intersect(s, other)
}

func (s set[T]) Diff(other ...Set[T]) Set[T] {
	return diff(s, other)
}

func (s set[T]) SymmetricDiff(other ...Set[T]) Set[T] {
	return symmetricDiff(s, other)
}

func (s set[T]) Equal(other ...Set[T]) bool {
	return s.SymmetricDiff(other...).Len() == 0
}

func (s set[T]) Len() int {
	return len(s)
}

func newSet[T comparable]() set[T] {
	return make(map[T]struct{})
}

func newOrdered[T comparable]() *Ordered[T] {
	return &Ordered[T]{set: newSet[T](), valByIdx: make(map[int]T)}
}
