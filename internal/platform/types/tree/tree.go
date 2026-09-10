package tree

import "iter"

type (
	Node[T any] struct {
		Data     T
		Children []*Node[T]
	}
)

func New[T any](data T) *Node[T] {
	return &Node[T]{
		Data:     data,
		Children: []*Node[T]{},
	}
}

func (n *Node[T]) AddChild(child *Node[T]) {
	n.Children = append(n.Children, child)
}

func (n *Node[T]) IsLeaf() bool {
	return len(n.Children) == 0
}

func (tree *Node[T]) DepthFirst() iter.Seq[T] {
	return func(yield func(T) bool) {
		if tree == nil {
			return
		}

		var walk func(*Node[T]) bool
		walk = func(node *Node[T]) bool {
			if !yield(node.Data) {
				return false
			}

			for _, child := range node.Children {
				if child != nil && !walk(child) {
					return false
				}
			}

			return true
		}

		walk(tree)
	}
}
