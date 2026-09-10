package query

import (
	"fmt"
	"iter"
	"strings"

	"github.com/sergisimo/ledger/internal/platform/fields"
	"github.com/sergisimo/ledger/internal/platform/filter"
	"github.com/sergisimo/ledger/internal/platform/types/tree"
)

// --------------------------------------------------------------- Contract

type (
	CondNode struct {
		Op     logicalOp
		Filter filter.FieldFilter[any]
	}

	Filters struct {
		condTree *tree.Node[CondNode]
	}

	logicalOp uint
)

const (
	FieldNameFilters fields.Name = "filters"

	OpUnknown logicalOp = iota
	OpAnd
	OpOr
)

// --------------------------------------------------------------- Implementation

func (qf Filters) Exists(keys ...fields.Name) bool {
	if len(keys) < 1 {
		panic("exists called without any keys")
	}
	for _, k := range keys {
		for node := range qf.Traverse() {
			if node.Filter != nil && node.Filter.Name() == k {
				return true
			}
		}
	}
	return false
}

func (qf Filters) Traverse() iter.Seq[CondNode] {
	return qf.condTree.DepthFirst()
}

func (qf Filters) String() string {
	return qf.conditionString(qf.condTree, false)
}

func (qf Filters) conditionString(node *tree.Node[CondNode], grouped bool) string {
	if node == nil {
		return ""
	}

	if node.IsLeaf() {
		filter := node.Data.Filter
		return fmt.Sprintf("%s %s %v", filter.Name(), filter.Operator(), filter.Value())
	}

	parts := make([]string, 0, len(node.Children))
	for _, child := range node.Children {
		if expression := qf.conditionString(child, true); expression != "" {
			parts = append(parts, expression)
		}
	}

	if len(parts) == 0 {
		return ""
	}
	if len(parts) == 1 {
		return parts[0]
	}

	operator := ""
	switch node.Data.Op {
	case OpAnd:
		operator = " AND "
	case OpOr:
		operator = " OR "
	}

	expression := strings.Join(parts, operator)
	if grouped {
		return "(" + expression + ")"
	}

	return expression
}

func Filter(f Filters) SrchOption {
	return func(s *search) {
		if s.filters.condTree == nil {
			s.filters = f
			return
		}

		s.filters = And(s.filters, f)
	}
}

func Where[T any](fieldName fields.Name, operator filter.Operator, val T) Filters {
	fltr := filter.NewFieldFilter[any](operator, fieldName, val)

	return Filters{
		condTree: tree.New(CondNode{Op: OpUnknown, Filter: fltr}),
	}
}

func Or(filters ...Filters) Filters {
	return combine(OpOr, filters...)
}

func And(filters ...Filters) Filters {
	return combine(OpAnd, filters...)
}

func combine(op logicalOp, conds ...Filters) Filters {
	if len(conds) == 0 {
		return Filters{}
	}
	if len(conds) == 1 {
		return conds[0]
	}

	parentNode := tree.New(CondNode{Op: op})
	for _, c := range conds {
		if c.condTree != nil {
			parentNode.AddChild(c.condTree)
		}
	}

	return Filters{condTree: parentNode}
}
