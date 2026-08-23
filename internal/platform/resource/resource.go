package resource

import "time"

// --------------------------------------------------------------- Contract

type (
	Identifier interface {
		ID() string
		Type() Type
	}

	Resource interface {
		Identifier
		CreatedAt() time.Time
		UpdatedAt() time.Time
		DeletedAt() *time.Time
	}

	List[T Resource] interface {
		Result() []T
		TotalCollSize() uint
	}

	Type string
)
