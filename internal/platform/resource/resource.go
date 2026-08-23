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

func (t Type) String() string {
	return string(t)
}

// --------------------------------------------------------------- Rest

type RestDTO struct {
	RID        string     `json:"id"`
	RType      Type       `json:"type"`
	RCreatedAt time.Time  `json:"created_at"`
	RUpdatedAt time.Time  `json:"updated_at"`
	RDeletedAt *time.Time `json:"deleted_at,omitempty"`
}

func ToRestDTO(r Resource) RestDTO {
	return RestDTO{
		RID:        r.ID(),
		RType:      r.Type(),
		RCreatedAt: r.CreatedAt(),
		RUpdatedAt: r.UpdatedAt(),
		RDeletedAt: r.DeletedAt(),
	}
}

func (r *RestDTO) ID() string {
	return r.RID
}

func (r *RestDTO) Type() Type {
	return r.RType
}

func (r *RestDTO) CreatedAt() time.Time {
	return r.RCreatedAt
}

func (r *RestDTO) UpdatedAt() time.Time {
	return r.RUpdatedAt
}

func (r *RestDTO) DeletedAt() *time.Time {
	return r.RDeletedAt
}
