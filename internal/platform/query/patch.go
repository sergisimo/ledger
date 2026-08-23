package query

import (
	"maps"
	"slices"

	"github.com/sergisimo/ledger/internal/platform/fields"
)

// --------------------------------------------------------------- Contract

type (
	PatchQuery interface {
		SearchOpts() []SrchOption
		Fields() PatchFields
	}

	PatchFields map[fields.Name]PatchField

	PatchField interface {
		Value() any
		Operation() PatchFieldOperation
	}

	PatchFieldOperation int32
)

const (
	PatchFieldOperationSet PatchFieldOperation = iota
	PatchFieldOperationAdd
	PatchFieldOperationRemove
)

func (op PatchFieldOperation) String() string {
	switch op {
	case PatchFieldOperationSet:
		return "SET"
	case PatchFieldOperationAdd:
		return "ADD"
	case PatchFieldOperationRemove:
		return "REMOVE"
	default:
		return "UNKNOWN"
	}
}

// --------------------------------------------------------------- Implementation

func (pf PatchFields) Exists(fName fields.Name) bool {
	_, exists := pf[fName]
	return exists
}

func (pf PatchFields) Set(fName fields.Name, val any, opts ...patchFieldOption) {
	v, ok := pf[fName]
	if ok {
		pf[fName] = updatePatchField(v, append(opts, patchFieldWithVal(val))...)
		return
	}
	pf[fName] = newPatchField(val, opts...)
}

func (pf PatchFields) Value(fName fields.Name) (any, bool) {
	field, exists := pf[fName]
	if !exists {
		return nil, false
	}

	return field.Value(), true
}

func (pf PatchFields) ValueOrNil(fName fields.Name) any {
	field, exists := pf[fName]
	if !exists {
		return nil
	}

	return field.Value()
}

func (pf PatchFields) Clone() PatchFields {
	return maps.Clone(pf)
}

func (pf PatchFields) Reduce(allow ...fields.Name) {
	for k := range pf {
		if !slices.Contains(allow, k) {
			delete(pf, k)
		}
	}
}

type (
	patch struct {
		patchFields map[fields.Name]PatchField
		searchOpts  []SrchOption
	}

	PatchOption func(pq *patch)
)

func NewPatch(opts ...PatchOption) *patch {
	pq := &patch{
		patchFields: make(map[fields.Name]PatchField),
	}
	for _, opt := range opts {
		opt(pq)
	}
	return pq
}

func (pq *patch) SearchOpts() []SrchOption {
	return pq.searchOpts
}

func (pq *patch) Fields() PatchFields {
	return pq.patchFields
}

func WithPatchQuery(query PatchQuery) PatchOption {
	return func(pq *patch) {
		pq.searchOpts = query.SearchOpts()
		pq.patchFields = query.Fields()
	}
}

func WithPatchFields(patchFields PatchFields) PatchOption {
	return func(pq *patch) {
		pq.patchFields = patchFields
	}
}

func PatchSearchOpts(opts ...SrchOption) PatchOption {
	return func(pq *patch) {
		pq.searchOpts = opts
	}
}

func Patch(name fields.Name, value any, opts ...patchFieldOption) PatchOption {
	return func(pq *patch) {
		pq.patchFields[name] = newPatchField(value, opts...)
	}
}

type (
	patchField struct {
		val any
		op  PatchFieldOperation
	}

	patchFieldOption func(c *patchField)
)

func defaultPatchFieldOpts() []patchFieldOption {
	return []patchFieldOption{PatchFieldSet}
}

func PatchFieldWithOperator(op PatchFieldOperation) patchFieldOption {
	return func(c *patchField) {
		c.op = op
	}
}

func patchFieldWithVal(val any) patchFieldOption {
	return func(c *patchField) {
		c.val = val
	}
}

func PatchFieldSet(field *patchField) {
	PatchFieldWithOperator(PatchFieldOperationSet)(field)
}

func PatchFieldAdd(field *patchField) {
	PatchFieldWithOperator(PatchFieldOperationAdd)(field)
}

func PatchFieldRemove(field *patchField) {
	PatchFieldWithOperator(PatchFieldOperationRemove)(field)
}

func newPatchField(value any, opts ...patchFieldOption) *patchField {
	field := new(patchField)
	for _, opt := range append(defaultPatchFieldOpts(), opts...) {
		opt(field)
	}

	return &patchField{
		val: value,
		op:  field.op,
	}
}

func updatePatchField(field PatchField, opts ...patchFieldOption) *patchField {
	updated := &patchField{
		val: field.Value(),
		op:  field.Operation(),
	}
	for _, opt := range opts {
		opt(updated)
	}

	return updated
}

func (pf *patchField) Value() any {
	return pf.val
}

func (pf *patchField) Operation() PatchFieldOperation {
	return pf.op
}
