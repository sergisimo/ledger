package tree_test

import (
	"slices"
	"testing"

	"github.com/sergisimo/ledger/internal/platform/types/tree"
	"github.com/stretchr/testify/assert"
)

func TestTree(t *testing.T) {
	t.Run("New", func(t *testing.T) {
		node := tree.New("root")

		assert.Equal(t, "root", node.Data)
		assert.Empty(t, node.Children)
	})

	t.Run("AddChild", func(t *testing.T) {
		parent := tree.New("parent")
		child := tree.New("child")

		parent.AddChild(child)

		assert.Equal(t, []*tree.Node[string]{child}, parent.Children)
	})

	t.Run("IsLeaf", func(t *testing.T) {
		leaf := tree.New("leaf")
		parent := tree.New("parent")
		parent.AddChild(leaf)

		assert.True(t, leaf.IsLeaf())
		assert.False(t, parent.IsLeaf())
	})

	t.Run("DepthFirst", func(t *testing.T) {
		t.Run("basic tree", func(t *testing.T) {
			root := tree.New("root")
			left := tree.New("left")
			left.AddChild(tree.New("left.left"))
			root.AddChild(left)
			root.AddChild(tree.New("right"))

			got := slices.Collect(root.DepthFirst())

			assert.Equal(t, []string{"root", "left", "left.left", "right"}, got)
		})

		t.Run("nil root", func(t *testing.T) {
			var root *tree.Node[string]

			assert.Empty(t, slices.Collect(root.DepthFirst()))
		})

		t.Run("yield false", func(t *testing.T) {
			root := tree.New(1)
			root.AddChild(tree.New(2))
			root.AddChild(tree.New(3))

			var got []int
			for value := range root.DepthFirst() {
				got = append(got, value)
				if value == 2 {
					break
				}
			}

			assert.Equal(t, []int{1, 2}, got)
		})
	})
}
