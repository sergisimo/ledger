package query

// --------------------------------------------------------------- Contract
type DeleteType int

const (
	DeleteTypeSoft DeleteType = iota
	DeleteTypeHard
)
