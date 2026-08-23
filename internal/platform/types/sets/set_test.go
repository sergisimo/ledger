package sets_test

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sergisimo/ledger/internal/platform/types/sets"
	"github.com/sergisimo/ledger/internal/platform/types/sets/setstest"
)

func Example() {
	set := sets.New(sets.With(1, 1, 1))
	fmt.Printf("initial set: %+v\n", set)

	set.Add(3, 6, 2, 13)
	fmt.Printf("after adding 3,6,2,13 set is: %+v\n", sets.SortedValues(set))
	fmt.Printf("set contains '2' is %t\n", set.Has(2))

	set.Delete(2, 6)
	fmt.Printf("after removing 2, 6 set values are: %+v\n", sets.SortedValues(set))
	fmt.Printf("so set contains '2' and '6' is {%t,%t}\n\n", set.Has(2), set.Has(6))

	orderedSet := sets.New(sets.With(3, 4, 1), sets.KeepOrder)
	fmt.Printf("initial Ordered set vals: %+v\n", orderedSet.Values())
	orderedSet.Add(4, 3)
	fmt.Printf("if we try to add 4, 3 (already present) set remains the same: %+v\n", orderedSet.Values())
	orderedSet.Delete(3)
	fmt.Printf("we delete 3 (first element), remaining: %+v\n", orderedSet.Values())
	orderedSet.Add(9, 6, 13, 69)
	fmt.Printf("we add 9, 6, 13, 69 and set is: %+v\n", orderedSet.Values())
	orderedSet.Delete(9, 6, 69)
	fmt.Printf("we remove 9, 6, 69 and set is: %+v\n", orderedSet.Values())
	orderedSet.Delete(109)
	fmt.Printf("we try remove unexisting value 109 and set is: %+v\n", orderedSet.Values())
	fmt.Printf("sorted result: %+v\n\n", sets.SortedValues(orderedSet))

	fmt.Printf(
		"intersect('%v', '%v') = '%v'\n\n",
		orderedSet.Values(), sets.SortedValues(set),
		orderedSet.Intersect(set).Values(),
	)

	// Output: initial set: map[1:{}]
	// after adding 3,6,2,13 set is: [1 2 3 6 13]
	// set contains '2' is true
	// after removing 2, 6 set values are: [1 3 13]
	// so set contains '2' and '6' is {false,false}
	//
	// initial Ordered set vals: [3 4 1]
	// if we try to add 4, 3 (already present) set remains the same: [3 4 1]
	// we delete 3 (first element), remaining: [4 1]
	// we add 9, 6, 13, 69 and set is: [4 1 9 6 13 69]
	// we remove 9, 6, 69 and set is: [4 1 13]
	// we try remove unexisting value 109 and set is: [4 1 13]
	// sorted result: [1 4 13]
	//
	// intersect('[4 1 13]', '[1 3 13]') = '[1 13]'
}

func TestMarshalUnmarshal(t *testing.T) {
	t.Parallel()

	type colour string

	const (
		colourRed   colour = "red"
		colourGreen colour = "green"
		colourBlue  colour = "blue"
	)
	rgbColours := []colour{colourRed, colourGreen, colourBlue}

	marshalTest := func(t *testing.T, s sets.Set[colour], want string) {
		t.Helper()
		marshalled, err := json.Marshal(s)
		require.NoError(t, err)
		assert.Equal(t, want, string(marshalled))
	}
	unmarshalTest := func(t *testing.T, in string, want sets.Set[colour]) {
		t.Helper()
		unmarshalledSet := new(sets.Ordered[colour])
		err := json.Unmarshal([]byte(in), unmarshalledSet)
		require.NoError(t, err)
		if in == "null" {
			assert.Empty(t, unmarshalledSet.Values())
			return
		}
		setstest.AssertEqual(t, want, unmarshalledSet)
	}

	tests := []struct {
		name    string
		in      sets.Set[colour]
		wantStr string
	}{
		{
			"set with rgb colours",
			sets.New(sets.With(rgbColours...), sets.KeepOrder),
			`["red","green","blue"]`,
		},
		{
			"set with no colours",
			sets.New[colour](sets.KeepOrder),
			`[]`,
		},
		{
			"nil set",
			nil,
			`null`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			marshalTest(t, tt.in, tt.wantStr)
			unmarshalTest(t, tt.wantStr, tt.in)
		})
	}
}

func TestSetDiff(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		first      sets.Set[string]
		other      []sets.Set[string]
		assertions func(t *testing.T, gotVals []string)
	}{
		{
			name:  "diff with no other sets returns same elements",
			first: sets.New(sets.With("apple", "cherry", "banana"), sets.KeepOrder),
			other: []sets.Set[string]{},
			assertions: func(t *testing.T, gotVals []string) {
				t.Helper()

				assert.Equal(t, []string{"apple", "cherry", "banana"}, gotVals)
			},
		},
		{
			name:  "diff with an empty set returns same elements",
			first: sets.New(sets.With("apple", "cherry", "banana"), sets.KeepOrder),
			other: []sets.Set[string]{sets.New[string]()},
			assertions: func(t *testing.T, gotVals []string) {
				t.Helper()

				assert.Equal(t, []string{"apple", "cherry", "banana"}, gotVals)
			},
		},
		{
			name:  "diff with elements contained in different sets returns elements diff",
			first: sets.New(sets.With("apple", "cherry", "banana", "strawberry"), sets.KeepOrder),
			other: []sets.Set[string]{
				sets.New(sets.With("watermelon", "pear", "apple")),
				sets.New(sets.With("cherry")),
			},
			assertions: func(t *testing.T, gotVals []string) {
				t.Helper()

				assert.Equal(t, []string{"banana", "strawberry"}, gotVals)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.assertions(t, tt.first.Diff(tt.other...).Values())
		})
	}
}

func TestSetSymmetricDiff(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		first sets.Set[string]
		other []sets.Set[string]
		want  sets.Set[string]
	}{
		{
			name:  "diff with no other sets returns same elements",
			first: sets.New(sets.With("apple", "cherry", "banana"), sets.KeepOrder),
			other: []sets.Set[string]{},
			want:  sets.New(sets.With("apple", "cherry", "banana"), sets.KeepOrder),
		},
		{
			name:  "diff with an empty set returns same elements",
			first: sets.New(sets.With("apple", "cherry", "banana"), sets.KeepOrder),
			other: []sets.Set[string]{sets.New[string]()},
			want:  sets.New(sets.With("apple", "cherry", "banana"), sets.KeepOrder),
		},
		{
			name:  "diff with elements contained in different sets returns elements symmetric diff",
			first: sets.New(sets.With("apple", "cherry", "banana", "strawberry"), sets.KeepOrder),
			other: []sets.Set[string]{
				sets.New(sets.With("watermelon", "pear", "apple")),
				sets.New(sets.With("cherry")),
			},
			want: sets.New(sets.With("banana", "strawberry", "watermelon", "pear"), sets.KeepOrder),
		},
		{
			name:  "diff with same elements returns empty set",
			first: sets.New(sets.With("apple", "cherry", "banana", "strawberry"), sets.KeepOrder),
			other: []sets.Set[string]{
				sets.New(sets.With("cherry", "banana", "apple", "strawberry")),
			},
			want: sets.New[string](sets.KeepOrder),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			res := test.first.SymmetricDiff(test.other...)
			assert.ElementsMatch(t, res.Values(), test.want.Values())

			assert.Equal(t, test.first.Equal(test.other...), len(res.Values()) == 0)
		})
	}
}

func TestSetIntersect(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		first      sets.Set[string]
		second     sets.Set[string]
		assertions func(t *testing.T, gotVals []string)
	}{
		{
			name:   "ordered set intersects with common elements",
			first:  sets.New(sets.With("SMS", "WHATSAPP", "FB_MESSENGER"), sets.KeepOrder),
			second: sets.New(sets.With("FB_MESSENGER", "SMS")),
			assertions: func(t *testing.T, gotVals []string) {
				t.Helper()

				assert.Equal(t, []string{"SMS", "FB_MESSENGER"}, gotVals)
			},
		},
		{
			name:   "unordered set intersects with ordered set common elements",
			first:  sets.New(sets.With("SMS", "FB_MESSENGER"), sets.Unordered),
			second: sets.New(sets.With("WHATSAPP", "FB_MESSENGER", "SMS"), sets.KeepOrder),
			assertions: func(t *testing.T, gotVals []string) {
				t.Helper()

				assert.ElementsMatch(t, []string{"FB_MESSENGER", "SMS"}, gotVals)
			},
		},
		{
			name:   "no intersection",
			first:  sets.New(sets.With("SMS"), sets.KeepOrder),
			second: sets.New(sets.With("WHATSAPP", "FB_MESSENGER")),
			assertions: func(t *testing.T, gotVals []string) {
				t.Helper()

				assert.Empty(t, gotVals)
			},
		},
		{
			name:   "both unordered full overlap",
			first:  sets.New(sets.With("SMS", "WHATSAPP"), sets.Unordered),
			second: sets.New(sets.With("WHATSAPP", "SMS"), sets.Unordered),
			assertions: func(t *testing.T, gotVals []string) {
				t.Helper()

				assert.ElementsMatch(t, []string{"WHATSAPP", "SMS"}, gotVals)
			},
		},
		{
			name:   "empty first set",
			first:  sets.New[string](),
			second: sets.New(sets.With("SMS"), sets.Unordered),
			assertions: func(t *testing.T, gotVals []string) {
				t.Helper()

				assert.Empty(t, gotVals)
			},
		},
		{
			name:   "empty second set",
			first:  sets.New(sets.With("SMS")),
			second: sets.New(sets.With([]string{}...)),
			assertions: func(t *testing.T, gotVals []string) {
				t.Helper()

				assert.Empty(t, gotVals)
			},
		},
		{
			name:   "both empty",
			first:  sets.New(sets.With([]string{}...)),
			second: sets.New(sets.With([]string{}...), sets.Unordered),
			assertions: func(t *testing.T, gotVals []string) {
				t.Helper()

				assert.Empty(t, gotVals)
			},
		},
		{
			name:   "single intersection",
			first:  sets.New(sets.With("WHATSAPP", "WHATSAPP", "SMS"), sets.KeepOrder),
			second: sets.New(sets.With("WHATSAPP", "WHATSAPP")),
			assertions: func(t *testing.T, gotVals []string) {
				t.Helper()

				assert.Equal(t, []string{"WHATSAPP"}, gotVals)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.assertions(t, tt.first.Intersect(tt.second).Values())
		})
	}
}

func ExampleSet_Intersect() {
	firstSet := sets.New(sets.With(3, 1, 4, 9, 7, 333, 428, 99999))
	secondSet := sets.New(sets.With(1, 69, 195, 7, 4))
	thirdSet := sets.New(sets.With(69, 4, 123, 1, 10003, 10004))
	fmt.Printf("set intersection: %+v", sets.SortedValues(
		firstSet.Intersect(secondSet, thirdSet),
	))

	// Output: set intersection: [1 4]
}

func ExampleSet_Diff() {
	firstSet := sets.New(sets.With(3, 1, 4, 9, 7, 333, 428, 99999), sets.KeepOrder)
	secondSet := sets.New(sets.With(1, 69, 195, 4))
	thirdSet := sets.New(sets.With(69, 4, 123, 1, 10003, 10004))
	fmt.Printf("set diff: %+v", sets.SortedValues(
		firstSet.Diff(secondSet, thirdSet),
	))

	// Output: set diff: [3 7 9 333 428 99999]
}

func TestNew(t *testing.T) {
	t.Parallel()

	type test struct {
		name   string
		opts   []sets.Option[int]
		assert func(t *testing.T, got sets.Set[int])
	}

	newOrderNotImportantSetTest := func(name string, initialVals, want []int) *test {
		const (
			orderNotImportantSetName = "set where insert order is not important"
		)
		return &test{
			name: orderNotImportantSetName + name,
			opts: []sets.Option[int]{sets.With(initialVals...)},
			assert: func(t *testing.T, got sets.Set[int]) {
				t.Helper()

				assert.Equal(t, len(want), got.Len())
				assert.ElementsMatch(t, want, got.Values())
			},
		}
	}

	newOrderedSetTest := func(name string, initialVals, want []int) *test {
		const (
			orderNotImportantSetName = "set where insert order is important"
		)
		return &test{
			name: orderNotImportantSetName + name,
			opts: []sets.Option[int]{sets.With(initialVals...), sets.KeepOrder[int]},
			assert: func(t *testing.T, got sets.Set[int]) {
				t.Helper()

				assert.Equal(t, len(want), got.Len())
				assert.Equal(t, want, got.Values())
			},
		}
	}

	testCases := []*test{
		newOrderNotImportantSetTest(
			"no duplicates",
			[]int{1, 2, 3, 4, 5},
			[]int{1, 2, 3, 4, 5},
		),
		newOrderNotImportantSetTest(
			"with duplicates",
			[]int{3, 1, 2, 2, 3, 4, 4, 5},
			[]int{1, 2, 3, 4, 5},
		),
		newOrderNotImportantSetTest(
			"all duplicates",
			[]int{1, 1, 1, 1, 1},
			[]int{1},
		),
		newOrderNotImportantSetTest(
			"empty slice",
			[]int{},
			[]int{},
		),
		newOrderNotImportantSetTest(
			"single element",
			[]int{1},
			[]int{1},
		),
		newOrderedSetTest(
			"no duplicates",
			[]int{1, 2, 3, 4, 5},
			[]int{1, 2, 3, 4, 5},
		),
		newOrderedSetTest(
			"with duplicates",
			[]int{3, 1, 2, 2, 3, 4, 4, 5},
			[]int{3, 1, 2, 4, 5},
		),
		newOrderedSetTest(
			"all duplicates",
			[]int{1, 1, 1, 1, 1},
			[]int{1},
		),
		newOrderedSetTest(
			"empty slice",
			[]int{},
			[]int{},
		),
		newOrderedSetTest(
			"single element",
			[]int{1},
			[]int{1},
		),
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			result := sets.New(tc.opts...)
			tc.assert(t, result)
		})
	}
}
