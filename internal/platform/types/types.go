// Package types contains helpers and types reused across our projects
// (some of these types are not covered by golang, i.e. sets).
package types

import (
	"encoding/json"
	"fmt"
	"strings"
)

type (
	Enum interface {
		~string
		fmt.Stringer
	}

	Uppercased string
)

func (u Uppercased) String() string {
	return string(u)
}

func (u Uppercased) MarshalJSON() ([]byte, error) {
	return json.Marshal(strings.ToUpper(string(u)))
}

func (u Uppercased) MarshalJSONAPIField() ([]byte, error) {
	return u.MarshalJSON()
}

func (u *Uppercased) UnmarshalJSONAPIField(data []byte) error {
	return u.UnmarshalJSON(data)
}

func (u *Uppercased) UnmarshalJSON(data []byte) error {
	var val string
	if err := json.Unmarshal(data, &val); err != nil {
		return err
	}
	*u = Uppercased(strings.ToUpper(val))

	return nil
}
