package rest

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"

	"github.com/sergisimo/ledger/internal/platform/fields"
	"github.com/sergisimo/ledger/internal/platform/filter"
	"github.com/sergisimo/ledger/internal/platform/query"
)

var (
	ErrInvalidFilterFormat = errors.New("filter format should be filter[field][operator]")
	ErrInvalidOperator     = errors.New("invalid operator")

	//nolint:gochecknoglobals // map is used in every GET request with filters, it's more efficient to keep it global
	Operators = map[string]filter.Operator{
		"eq":       filter.OpEq,
		"ne":       filter.OpNEq,
		"gt":       filter.OpGT,
		"gte":      filter.OpGTEq,
		"lt":       filter.OpLT,
		"lte":      filter.OpLTEq,
		"in":       filter.OpIn,
		"not-in":   filter.OpNotIn,
		"like":     filter.OpLike,
		"btw":      filter.OpBetween,
		"any":      filter.OpContains,
		"not-any":  filter.OpNotContains,
		"any-like": filter.OpContainsLike,
		"is":       filter.OpIs,
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

	return append(opts, query.FilterBy(fields.NameID, filter.OpEq, id)), nil
}

func parseURLSrchOpts(uri *url.URL) ([]query.SrchOption, error) {
	opts, err := searchFromURL(uri)
	if err != nil {
		return nil, err
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

func searchFromURL(uri *url.URL) ([]query.SrchOption, error) {
	opts := []query.SrchOption{}
	for key, values := range uri.Query() {
		if strings.Contains(key, "filter") {
			fName, op, err := parseFilter(key)
			if err != nil {
				return nil, err
			}
			opts = append(opts, query.FilterBy(fName, op, parseValue(op, values)))
		}
	}

	return opts, nil
}

func parseFilter(filterKey string) (fields.Name, filter.Operator, error) {
	const filterSplits = 3

	split := strings.Split(filterKey, "[")
	if len(split) != filterSplits {
		return "", filter.OpUndefined, fields.NewErrWithFieldName(fields.Name(filterKey), ErrInvalidFilterFormat)
	}

	fName := strings.ReplaceAll(split[1], "]", "")
	op := parseOperator(strings.ReplaceAll(split[2], "]", ""))
	if op == filter.OpUndefined {
		return "", filter.OpUndefined, fields.NewErrWithFieldName(fields.Name(fName), ErrInvalidOperator)
	}

	return fields.Name(fName), op, nil
}

func parseOperator(val string) filter.Operator {
	v, ok := Operators[val]
	if !ok {
		return filter.OpUndefined
	}
	return v
}

func parseValue(op filter.Operator, val []string) any {
	if len(val) == 1 {
		if val[0] == "null" {
			return nil
		}
		match, err := regexp.MatchString("^(?i)(true|false)$", val[0])
		if err != nil {
			return val[0]
		}
		if match {
			if b, err := strconv.ParseBool(val[0]); err == nil {
				return b
			}
		}
		if strings.Contains(val[0], ",") {
			return strings.Split(val[0], ",")
		} else if op == filter.OpIn || op == filter.OpContainsLike {
			if val[0] == "" {
				return []string{}
			}
			return []string{val[0]}
		}
		return val[0]
	}
	return val
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
