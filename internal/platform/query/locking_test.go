package query_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/sergisimo/ledger/internal/platform/query"
)

func TestLockStrength(t *testing.T) {
	tests := []struct {
		name      string
		strength  query.LockStrength
		wantStr   string
		wantValid bool
	}{
		{
			name:      "update",
			strength:  query.LockStrengthUpdate,
			wantStr:   "UPDATE",
			wantValid: true,
		},
		{
			name:      "share",
			strength:  query.LockStrengthShare,
			wantStr:   "SHARE",
			wantValid: true,
		},
		{
			name:      "out of range",
			strength:  query.LockStrength(99),
			wantStr:   "",
			wantValid: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.wantStr, tc.strength.String())
			assert.Equal(t, tc.wantValid, tc.strength.Valid())
		})
	}
}

func TestLockWait(t *testing.T) {
	tests := []struct {
		name      string
		wait      query.LockWait
		wantStr   string
		wantValid bool
	}{
		{
			name:      "block",
			wait:      query.LockWaitBlock,
			wantStr:   "",
			wantValid: true,
		},
		{
			name:      "error",
			wait:      query.LockWaitError,
			wantStr:   "NOWAIT",
			wantValid: true,
		},
		{
			name:      "skip",
			wait:      query.LockWaitSkip,
			wantStr:   "SKIP LOCKED",
			wantValid: true,
		},
		{
			name:      "out of range",
			wait:      query.LockWait(99),
			wantStr:   "",
			wantValid: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.wantStr, tc.wait.String())
			assert.Equal(t, tc.wantValid, tc.wait.Valid())
		})
	}
}

func TestLock(t *testing.T) {
	tests := []struct {
		name         string
		opt          query.SrchOption
		wantStrength query.LockStrength
		wantWait     query.LockWait
		wantStr      string
	}{
		{
			name:         "defaults to blocking",
			opt:          query.Lock(query.LockStrengthUpdate),
			wantStrength: query.LockStrengthUpdate,
			wantWait:     query.LockWaitBlock,
			wantStr:      "locking: UPDATE",
		},
		{
			name:         "with nowait",
			opt:          query.Lock(query.LockStrengthUpdate, query.LockWaitOn(query.LockWaitError)),
			wantStrength: query.LockStrengthUpdate,
			wantWait:     query.LockWaitError,
			wantStr:      "locking: UPDATE NOWAIT",
		},
		{
			name:         "with skip locked",
			opt:          query.Lock(query.LockStrengthShare, query.LockWaitOn(query.LockWaitSkip)),
			wantStrength: query.LockStrengthShare,
			wantWait:     query.LockWaitSkip,
			wantStr:      "locking: SHARE SKIP LOCKED",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			locking := query.NewSearch(tc.opt).Locking()

			assert.Equal(t, tc.wantStrength, locking.Strength())
			assert.Equal(t, tc.wantWait, locking.Wait())
			assert.Equal(t, tc.wantStr, locking.String())
		})
	}
}
