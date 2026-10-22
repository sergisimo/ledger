package query

import (
	"fmt"
	"strings"

	"github.com/sergisimo/ledger/internal/platform/fields"
)

// --------------------------------------------------------------- Contract

type (
	LockingParams interface {
		fmt.Stringer
		Strength() LockStrength
		Wait() LockWait
	}

	LockStrength int32
	LockWait     int32
)

const (
	FieldNameLocking fields.Name = "locking"
)

const (
	LockStrengthUpdate LockStrength = iota
	LockStrengthShare
)

const (
	LockWaitBlock LockWait = iota
	LockWaitError
	LockWaitSkip
)

// --------------------------------------------------------------- Implementation

func (ls LockStrength) Valid() bool {
	return ls == LockStrengthUpdate || ls == LockStrengthShare
}

func (ls LockStrength) String() string {
	switch ls {
	case LockStrengthUpdate:
		return "UPDATE"
	case LockStrengthShare:
		return "SHARE"
	default:
		return ""
	}
}

func (lw LockWait) Valid() bool {
	return lw == LockWaitBlock || lw == LockWaitError || lw == LockWaitSkip
}

func (lw LockWait) String() string {
	switch lw {
	case LockWaitError:
		return "NOWAIT"
	case LockWaitSkip:
		return "SKIP LOCKED"
	case LockWaitBlock:
		return ""
	default:
		return ""
	}
}

type lockingParams struct {
	strength LockStrength
	wait     LockWait
}

func (lp *lockingParams) Strength() LockStrength {
	return lp.strength
}

func (lp *lockingParams) Wait() LockWait {
	return lp.wait
}

func (lp *lockingParams) String() string {
	parts := make([]string, 0, 2) //nolint:mnd // a lock is described by its strength and its wait behaviour
	if strength := lp.strength.String(); strength != "" {
		parts = append(parts, strength)
	}
	if wait := lp.wait.String(); wait != "" {
		parts = append(parts, wait)
	}

	return fmt.Sprintf("locking: %s", strings.Join(parts, " "))
}

func Lock(strength LockStrength, opts ...LockOption) SrchOption {
	return func(s *search) {
		locking := &lockingParams{strength: strength, wait: LockWaitBlock}
		for _, opt := range opts {
			opt(locking)
		}

		s.locking = locking
	}
}

type LockOption func(lp *lockingParams)

func LockWaitOn(wait LockWait) LockOption {
	return func(lp *lockingParams) {
		lp.wait = wait
	}
}
