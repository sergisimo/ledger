package sqldb

import (
	"github.com/sergisimo/ledger/internal/platform/fields"
	"gorm.io/gorm"
)

type (
	Config struct {
		dialector gorm.Dialector
	}

	Option func(*Config)

	Dialect string
)

const (
	fieldNameDialector fields.Name = "dialector"
)

func WithDialector(d gorm.Dialector) Option {
	return func(c *Config) {
		c.dialector = d
	}
}

func NewClient(opts ...Option) (*gorm.DB, error) {
	cfg := &Config{}

	for _, opt := range opts {
		opt(cfg)
	}

	if cfg.dialector == nil {
		return nil, fields.NewErrNil(fieldNameDialector)
	}

	gormDB, err := gorm.Open(cfg.dialector, &gorm.Config{})
	if err != nil {
		return nil, err
	}

	return gormDB, nil
}
