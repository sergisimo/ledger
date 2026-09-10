package rest

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/sergisimo/ledger/internal/platform/fields"
	"github.com/sergisimo/ledger/internal/platform/filter"
	"github.com/sergisimo/ledger/internal/platform/query"
)

const (
	fieldNameAnd fields.Name = "and"
	fieldNameOr  fields.Name = "or"
)

var (
	ErrInvalidFilterFormat = errors.New("filter format should be a JSON condition tree")
	ErrInvalidOperator     = errors.New("invalid operator")

	//nolint:gochecknoglobals // map is used in every GET request with filters, it's more efficient to keep it global
	Operators = map[string]filter.Operator{
		"eq":           filter.OpEq,
		"ne":           filter.OpNEq,
		"neq":          filter.OpNEq,
		"gt":           filter.OpGT,
		"gte":          filter.OpGTEq,
		"gteq":         filter.OpGTEq,
		"lt":           filter.OpLT,
		"lte":          filter.OpLTEq,
		"lteq":         filter.OpLTEq,
		"in":           filter.OpIn,
		"not-in":       filter.OpNotIn,
		"notin":        filter.OpNotIn,
		"like":         filter.OpLike,
		"btw":          filter.OpBetween,
		"between":      filter.OpBetween,
		"any":          filter.OpContains,
		"contains":     filter.OpContains,
		"not-any":      filter.OpNotContains,
		"notcontains":  filter.OpNotContains,
		"any-like":     filter.OpContainsLike,
		"containslike": filter.OpContainsLike,
		"is":           filter.OpIs,
		"isnot":        filter.OpIsNot,
	}
)

func decodeGetReq(_ context.Context, req *http.Request) ([]query.SrchOption, error) {
	id := req.PathValue("id")
	if id == "" {
		return nil, fields.NewErrInvalidEmptyString(fields.NameID)
	}

	opts, err := parseURLSrchOpts(req.URL)
	if err != nil {
		return nil, err
	}

	return append(opts, query.Filter(query.Where(fields.NameID, filter.OpEq, id))), nil
}

func parseURLSrchOpts(uri *url.URL) ([]query.SrchOption, error) {
	var opts []query.SrchOption
	srch, err := searchFromURL(uri)
	if err != nil {
		return nil, err
	}
	if srch != nil {
		opts = append(opts, srch)
	}

	pag, err := paginationFromURL(uri)
	if err != nil {
		return nil, err
	}
	if pag != nil {
		opts = append(opts, pag)
	}

	sort, err := sortFromURL(uri)
	if err != nil {
		return nil, err
	}
	if sort != nil {
		opts = append(opts, sort...)
	}

	return opts, nil
}

func searchFromURL(uri *url.URL) (query.SrchOption, error) {
	filterValue := uri.Query().Get(string(filter.FieldNameFilter))
	if filterValue == "" {
		return nil, nil
	}

	filters, err := parseFilterTree([]byte(filterValue))
	if err != nil {
		return nil, err
	}

	return query.Filter(filters), nil
}

func parseFilterTree(data []byte) (query.Filters, error) {
	var condition map[string]any
	if err := json.Unmarshal(data, &condition); err != nil {
		return query.Filters{}, fields.NewErrWithFieldName(query.FieldNameFilters, ErrInvalidFilterFormat)
	}
	return parseFilterCondition(condition)
}

func parseFilterCondition(condition map[string]any) (query.Filters, error) {
	if len(condition) != 1 {
		return query.Filters{}, fields.NewErrWithFieldName(query.FieldNameFilters, ErrInvalidFilterFormat)
	}

	for key, value := range condition {
		if key == string(fieldNameAnd) || key == string(fieldNameOr) {
			children, ok := value.([]any)
			if !ok || len(children) == 0 {
				return query.Filters{}, fields.NewErrWithFieldName(query.FieldNameFilters, ErrInvalidFilterFormat)
			}

			parsed := make([]query.Filters, 0, len(children))
			for _, child := range children {
				childCondition, ok := child.(map[string]any)
				if !ok {
					return query.Filters{}, fields.NewErrWithFieldName(query.FieldNameFilters, ErrInvalidFilterFormat)
				}

				parsedChild, err := parseFilterCondition(childCondition)
				if err != nil {
					return query.Filters{}, err
				}
				parsed = append(parsed, parsedChild)
			}
			if key == string(fieldNameAnd) {
				return query.And(parsed...), nil
			}
			return query.Or(parsed...), nil
		}

		operators, ok := value.(map[string]any)
		if !ok || len(operators) != 1 {
			return query.Filters{}, fields.NewErrWithFieldName(fields.Name(key), ErrInvalidFilterFormat)
		}
		for operator, rawValue := range operators {
			op := parseOperator(operator)
			if op == filter.OpUndefined {
				return query.Filters{}, fields.NewErrWithFieldName(fields.Name(key), ErrInvalidOperator)
			}
			parsedValue := rawValue
			if values, ok := rawValue.([]any); ok {
				stringsValues := make([]string, len(values))
				for index, value := range values {
					stringValue, ok := value.(string)
					if !ok {
						break
					}
					stringsValues[index] = stringValue
				}
				if len(stringsValues) == len(values) {
					parsedValue = stringsValues
				}
			}
			return query.Where(fields.Name(key), op, parsedValue), nil
		}
	}

	return query.Filters{}, fields.NewErrWithFieldName(query.FieldNameFilters, ErrInvalidFilterFormat)
}

func parseOperator(val string) filter.Operator {
	v, ok := Operators[val]
	if !ok {
		return filter.OpUndefined
	}
	return v
}

func paginationFromURL(uri *url.URL) (opt query.SrchOption, err error) {
	limit := 0
	offset := 0
	l := uri.Query().Get("page[limit]")
	o := uri.Query().Get("page[offset]")
	if l == "" && o == "" {
		return nil, nil
	}
	if l != "" {
		limit, err = strconv.Atoi(l)
		if err != nil || limit < 0 {
			return nil, fields.NewErrInvalidValue(query.FieldNamePagination.Merge(fields.Name("limit")), l, err.Error())
		}
	}
	if o != "" {
		offset, err = strconv.Atoi(o)
		if err != nil || offset < 0 {
			return nil, fields.NewErrInvalidValue(query.FieldNamePagination.Merge(fields.Name("offset")), o, err.Error())
		}
	}
	return query.Pagination(limit, offset), nil
}

func sortFromURL(uri *url.URL) ([]query.SrchOption, error) {
	sortParam := uri.Query().Get("sort")
	if sortParam == "" {
		return nil, nil
	}

	opts := []query.SrchOption{}
	for _, s := range strings.Split(sortParam, ",") {
		if s == "" {
			return nil, fields.NewErrInvalidEmptyString(query.FieldNameSorting)
		}
		sortType := query.SortAsc
		fieldName := fields.Name(s)
		if strings.HasPrefix(s, "-") {
			sortType = query.SortDesc
			fieldName = fields.Name(strings.TrimPrefix(s, "-"))
		}
		opts = append(opts, query.SortBy(fieldName, sortType))
	}

	return opts, nil
}
