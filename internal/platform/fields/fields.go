package fields

import "fmt"

type Name string

const (
	NameID           Name = "id"
	NameCreationTime Name = "createdAt"
	NameUpdatedTime  Name = "updatedAt"
	NameDeletionTime Name = "deletedAt"

	NameName    Name = "name"
	NameEmail   Name = "email"
	NameDomain  Name = "domain"
	NameUsecase Name = "usecase"
	NameType    Name = "type"
	NameKind    Name = "kind"
	NameClient  Name = "client"
	NameConfig  Name = "config"
	NameService Name = "service"
)

func (f Name) String() string {
	return string(f)
}

func (f Name) Merge(children ...Name) Name {
	merged := f
	for _, child := range children {
		merged = Name(fmt.Sprintf("%s.%s", merged.String(), child.String()))
	}

	return merged
}

func Cast[T any](fName Name, v any) (T, error) {
	t, ok := v.(T)
	if !ok {
		var zero T
		return zero, NewErrInvalidType(fName, zero, v)
	}
	return t, nil
}
