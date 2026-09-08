package sqlite

import "github.com/jinzhu/gorm"

func NewClient() (*gorm.DB, error) {
	gormDB, err := gorm.Open("sqlite3", ":memory:")
	if err != nil {
		return nil, err
	}

	return gormDB, nil
}
