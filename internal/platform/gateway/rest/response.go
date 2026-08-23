package rest

import (
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
)

type (
	response struct {
		Data     any   `json:"data"`
		Included []any `json:"included,omitempty"`
	}
)

func encodeSingleJSONResponse(w http.ResponseWriter, statusCode int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)

	response := response{Data: data}
	if dataWithIncluded, ok := data.(interface{ Included() []any }); ok {
		response.Included = dataWithIncluded.Included()
		for _, included := range response.Included {
			rv := reflect.ValueOf(included)
			if rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array {
				panic(fmt.Sprintf("included resource %v cannot be a slice or array", included))
			}
		}

	}

	err := json.NewEncoder(w).Encode(response)
	if err != nil {
		encodeErrorResponse(w, http.StatusInternalServerError, err)
	}
}

func encodeListJSONResponse(w http.ResponseWriter, statusCode int, data []any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)

	response := response{Data: data}
	response.Included = []any{}
	for _, item := range data {
		if itemWithIncluded, ok := item.(interface{ Included() []any }); ok {
			response.Included = append(response.Included, itemWithIncluded.Included()...)
		}
	}

	for _, included := range response.Included {
		rv := reflect.ValueOf(included)
		if rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array {
			panic(fmt.Sprintf("included resource %v cannot be a slice or array", included))
		}
	}

	err := json.NewEncoder(w).Encode(response)
	if err != nil {
		encodeErrorResponse(w, http.StatusInternalServerError, err)
	}
}
