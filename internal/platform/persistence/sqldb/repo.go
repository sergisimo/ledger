package sqldb

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/sergisimo/ledger/internal/platform/fields"
	"github.com/sergisimo/ledger/internal/platform/filter"
	"github.com/sergisimo/ledger/internal/platform/persistence"
	"github.com/sergisimo/ledger/internal/platform/query"
	"github.com/sergisimo/ledger/internal/platform/types/tree"
)

const dialectPostgres = "postgres"

type Repo struct {
	DB      *gorm.DB
	fMapper map[fields.Name]string
}

func NewRepo(db *gorm.DB, fMapper map[fields.Name]string) *Repo {
	mapper := fMapper
	if mapper == nil {
		mapper = make(map[fields.Name]string)
	}

	return &Repo{
		DB:      db,
		fMapper: mapper,
	}
}

func (r *Repo) ApplySearch(ctx context.Context, qry query.Search) *gorm.DB {
	dbOp := r.session(ctx)
	if qry == nil {
		return dbOp
	}

	dbOp = r.applyFilters(dbOp, qry.Filters())
	dbOp = r.applySorting(dbOp, qry.Sorting())
	dbOp = r.applyPagination(dbOp, qry.Pagination())
	dbOp = r.applyLocking(ctx, dbOp, qry.Locking())

	return dbOp
}

func (r *Repo) session(ctx context.Context) *gorm.DB {
	if tx, ok := persistence.TxFromCtx[*gorm.DB](ctx); ok {
		return tx.WithContext(ctx)
	}

	return r.DB.WithContext(ctx)
}

func (r *Repo) applyLocking(ctx context.Context, op *gorm.DB, locking query.LockingParams) *gorm.DB {
	if locking == nil {
		return op
	}

	strength := locking.Strength()
	if !strength.Valid() {
		_ = op.AddError(NewErrInvalidLocking(fmt.Sprintf("strength %d is not a valid lock strength", strength)))
		return op
	}

	wait := locking.Wait()
	if !wait.Valid() {
		_ = op.AddError(NewErrInvalidLocking(fmt.Sprintf("wait %d is not a valid lock wait behaviour", wait)))
		return op
	}

	if _, ok := persistence.TxFromCtx[*gorm.DB](ctx); !ok {
		_ = op.AddError(ErrLockingWithoutTx)
		return op
	}

	return op.Clauses(clause.Locking{Strength: strength.String(), Options: wait.String()})
}

func (r *Repo) ApplyPatch(ctx context.Context, model any, qry query.PatchQuery) *gorm.DB {
	dbOp := r.session(ctx).Model(model)
	if qry == nil {
		_ = dbOp.AddError(ErrEmptyPatch)
		return dbOp
	}

	dbOp = r.applyFilters(dbOp, query.NewSearch(qry.SearchOpts()...).Filters())
	if dbOp.Error != nil {
		return dbOp
	}

	values, err := r.patchValues(qry.Fields())
	if err != nil {
		_ = dbOp.AddError(err)
		return dbOp
	}

	return dbOp.Updates(values)
}

func (r *Repo) patchValues(pFields query.PatchFields) (map[string]any, error) {
	if len(pFields) == 0 {
		return nil, ErrEmptyPatch
	}

	values := make(map[string]any, len(pFields))
	for name, pField := range pFields {
		col, err := r.column(name)
		if err != nil {
			return nil, err
		}

		value, err := r.patchValue(name, col, pField)
		if err != nil {
			return nil, err
		}

		values[col.Name] = value
	}

	return values, nil
}

func (r *Repo) patchValue(name fields.Name, col clause.Column, pField query.PatchField) (any, error) {
	val := pField.Value()

	switch pField.Operation() {
	case query.PatchFieldOperationSet:
		return val, nil
	case query.PatchFieldOperationAdd:
		return r.addValue(name, col, val)
	case query.PatchFieldOperationRemove:
		return r.removeValue(name, col, val)
	default:
		return nil, NewErrUnsupportedPatchOperation(name, pField.Operation())
	}
}

func (r *Repo) addValue(name fields.Name, col clause.Column, val any) (any, error) {
	if isNumeric(val) {
		return clause.Expr{SQL: "? + ?", Vars: []any{col, val}}, nil
	}

	if isCollection(val) {
		if err := r.requirePostgres(name, query.PatchFieldOperationAdd); err != nil {
			return nil, err
		}
		return clause.Expr{SQL: "array_cat(?, ?)", Vars: []any{col, val}}, nil
	}

	return nil, NewErrInvalidPatchValue(name, query.PatchFieldOperationAdd, val, "only numbers and collections can be added")
}

func (r *Repo) removeValue(name fields.Name, col clause.Column, val any) (any, error) {
	if val == nil {
		return nil, nil
	}

	if isNumeric(val) {
		return clause.Expr{SQL: "? - ?", Vars: []any{col, val}}, nil
	}

	if isCollection(val) {
		if err := r.requirePostgres(name, query.PatchFieldOperationRemove); err != nil {
			return nil, err
		}

		sql := "?"
		vars := []any{col}
		for _, elem := range collectionValues(val) {
			sql = "array_remove(" + sql + ", ?)"
			vars = append(vars, elem)
		}

		return clause.Expr{SQL: sql, Vars: vars}, nil
	}

	return nil, NewErrInvalidPatchValue(name, query.PatchFieldOperationRemove, val, "only null, numbers and collections can be removed")
}

func (r *Repo) requirePostgres(name fields.Name, op query.PatchFieldOperation) error {
	if r.DB == nil || r.DB.Dialector == nil || r.DB.Name() != dialectPostgres {
		return NewErrUnsupportedPatchOperation(name, op)
	}

	return nil
}

func (r *Repo) applyFilters(op *gorm.DB, filters query.Filters) *gorm.DB {
	if filters.Empty() {
		return op
	}

	expr, err := r.condition(filters.Root())
	if err != nil {
		_ = op.AddError(err)
		return op
	}

	if expr == nil {
		return op
	}

	return op.Where(expr)
}

func (r *Repo) condition(node *tree.Node[query.CondNode]) (clause.Expression, error) {
	if node == nil {
		return nil, nil
	}

	if node.IsLeaf() {
		if node.Data.Filter == nil {
			return nil, nil
		}

		return r.applyFilter(node.Data.Filter)
	}

	exprs := make([]clause.Expression, 0, len(node.Children))
	for _, child := range node.Children {
		expr, err := r.condition(child)
		if err != nil {
			return nil, err
		}
		if expr != nil {
			exprs = append(exprs, expr)
		}
	}

	if len(exprs) == 0 {
		return nil, nil
	}

	if len(exprs) == 1 {
		return exprs[0], nil
	}

	if node.Data.Op == query.OpOr {
		return clause.Or(exprs...), nil
	}

	return clause.And(exprs...), nil
}

func (r *Repo) applyFilter(fltr filter.FieldFilter[any]) (clause.Expression, error) {
	col, err := r.column(fltr.Name())
	if err != nil {
		return nil, err
	}

	val := fltr.Value()

	switch fltr.Operator() {
	case filter.OpEq:
		return clause.Eq{Column: col, Value: val}, nil
	case filter.OpNEq:
		return clause.Neq{Column: col, Value: val}, nil
	case filter.OpGT:
		return clause.Gt{Column: col, Value: val}, nil
	case filter.OpGTEq:
		return clause.Gte{Column: col, Value: val}, nil
	case filter.OpLT:
		return clause.Lt{Column: col, Value: val}, nil
	case filter.OpLTEq:
		return clause.Lte{Column: col, Value: val}, nil
	case filter.OpIn:
		values, err := sliceValues(fltr, val)
		if err != nil {
			return nil, err
		}
		return clause.IN{Column: col, Values: values}, nil
	case filter.OpNotIn:
		values, err := sliceValues(fltr, val)
		if err != nil {
			return nil, err
		}
		return clause.Not(clause.IN{Column: col, Values: values}), nil
	case filter.OpLike:
		pattern, err := stringValue(fltr, val)
		if err != nil {
			return nil, err
		}
		return clause.Like{Column: col, Value: pattern}, nil
	case filter.OpBetween:
		bounds, err := sliceValues(fltr, val)
		if err != nil {
			return nil, err
		}
		if len(bounds) != 2 {
			return nil, NewErrInvalidFilterValue(fltr.Name(), fltr.Operator(), val, "exactly two bounds are required")
		}
		return clause.Expr{SQL: "? BETWEEN ? AND ?", Vars: []any{col, bounds[0], bounds[1]}}, nil
	case filter.OpIs:
		return isExpr(col, fltr, val, false)
	case filter.OpIsNot:
		return isExpr(col, fltr, val, true)
	case filter.OpContains:
		return r.containsExpr(col, fltr, val, false)
	case filter.OpNotContains:
		return r.containsExpr(col, fltr, val, true)
	default:
		return nil, NewErrUnsupportedOperator(fltr.Name(), fltr.Operator())
	}
}

func (r *Repo) applySorting(op *gorm.DB, sorting query.SortingParams) *gorm.DB {
	if sorting == nil {
		return op
	}

	for _, name := range sorting.Keys() {
		dir := sorting.Get(name)
		if !dir.Valid() {
			continue
		}

		col, err := r.column(name)
		if err != nil {
			_ = op.AddError(err)
			return op
		}

		op = op.Order(clause.OrderByColumn{Column: col, Desc: dir == query.SortDesc})
	}

	return op
}

func (r *Repo) applyPagination(op *gorm.DB, pagination query.PaginationParams) *gorm.DB {
	if pagination == nil {
		return op
	}

	switch pagination.Kind() {
	case query.PaginationKindLimit:
		limitPagination, ok := pagination.(query.LimitPagination)
		if !ok {
			_ = op.AddError(NewErrUnsupportedPagination(pagination.Kind()))
			return op
		}

		if limitPagination.Limit() > 0 {
			op = op.Limit(int(limitPagination.Limit()))
		}
		if limitPagination.Offset() > 0 {
			op = op.Offset(int(limitPagination.Offset()))
		}

		return op
	default:
		_ = op.AddError(NewErrUnsupportedPagination(pagination.Kind()))
		return op
	}
}

func (r *Repo) column(name fields.Name) (clause.Column, error) {
	mapped, exists := r.fMapper[name]
	if !exists || mapped == "" {
		return clause.Column{}, NewErrUnmappedField(name)
	}

	return clause.Column{Name: mapped}, nil
}

func (r *Repo) containsExpr(col clause.Column, fltr filter.FieldFilter[any], val any, negated bool) (clause.Expression, error) {
	if r.DB == nil || r.DB.Dialector == nil || r.DB.Name() != dialectPostgres {
		return nil, NewErrUnsupportedOperator(fltr.Name(), fltr.Operator())
	}

	slice, err := sliceValues(fltr, val)
	if err != nil {
		return nil, err
	}

	expr := clause.Expr{SQL: "? @> ARRAY[?]", Vars: []any{col, slice}}
	if negated {
		return clause.Not(expr), nil
	}

	return expr, nil
}

func isExpr(col clause.Column, fltr filter.FieldFilter[any], val any, negated bool) (clause.Expression, error) {
	predicate := ""
	switch typed := val.(type) {
	case nil:
		predicate = "NULL"
	case bool:
		predicate = strings.ToUpper(fmt.Sprintf("%t", typed))
	default:
		return nil, NewErrInvalidFilterValue(fltr.Name(), fltr.Operator(), val, "only null and booleans are comparable with IS")
	}

	operator := "IS"
	if negated {
		operator = "IS NOT"
	}

	return clause.Expr{SQL: fmt.Sprintf("? %s %s", operator, predicate), Vars: []any{col}}, nil
}

func sliceValues(fltr filter.FieldFilter[any], val any) ([]any, error) {
	if !isCollection(val) {
		return nil, NewErrInvalidFilterValue(fltr.Name(), fltr.Operator(), val, "a collection of values is required")
	}

	return collectionValues(val), nil
}

func isNumeric(val any) bool {
	if val == nil {
		return false
	}

	switch reflect.ValueOf(val).Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64:
		return true
	default:
		return false
	}
}

func isCollection(val any) bool {
	if val == nil {
		return false
	}

	kind := reflect.ValueOf(val).Kind()
	return kind == reflect.Slice || kind == reflect.Array
}

func collectionValues(val any) []any {
	reflected := reflect.ValueOf(val)

	values := make([]any, 0, reflected.Len())
	for i := range reflected.Len() {
		values = append(values, reflected.Index(i).Interface())
	}

	return values
}

func stringValue(fltr filter.FieldFilter[any], val any) (string, error) {
	pattern, ok := val.(string)
	if !ok {
		return "", NewErrInvalidFilterValue(fltr.Name(), fltr.Operator(), val, "a string value is required")
	}

	return pattern, nil
}
