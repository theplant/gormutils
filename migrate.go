package gormutils

import (
	"fmt"

	"github.com/jinzhu/gorm"
	"github.com/theplant/appkit/log"
)

// Migratable defines interface for implementing post-migration
// actions such as adding constraints that arent's supported by Gorm's
// struct tags. This function must be idempotent, since it will most
// likely be executed multiple times.
type Migratable interface {
	AfterMigrate(db *gorm.DB) error
}

// ResetDB function will drop then auto migrate all the tables.
func ResetDB(l log.Logger, db *gorm.DB, tables ...interface{}) error {
	if err := Drop(l, db, tables...); err != nil {
		return err
	}

	return AutoMigrate(l, db, tables...)
}

// AutoMigrate receives table arguments and create or update their
// table structure in database.
func AutoMigrate(l log.Logger, db *gorm.DB, tables ...interface{}) error {
	l.Info().Log("msg", "running db auto-migration...")

	for _, table := range tables {
		name := fmt.Sprintf("%T", table)
		l.Debug().Log(
			"msg", fmt.Sprintf("auto-migrating %T", table),
			"table", name,
		)

		if err := db.AutoMigrate(table).Error; err != nil {
			l.Crit().Log(
				"during", "db/migrate.AutoMigrate",
				"err", err,
				"msg", fmt.Sprintf("error during auto migration: %v", err),
			)
			return err
		}

		if migratable, ok := table.(Migratable); ok {
			l.Debug().Log(
				"table", name,
				"msg", fmt.Sprintf("executing AfterMigrate for %T", migratable),
			)

			if err := migratable.AfterMigrate(db); err != nil {
				l.Crit().Log(
					"during", "migratable.AfterMigrate",
					"err", err,
					"msg", fmt.Sprintf("error during migratable callbacks: %v", err),
				)
				return err
			}
		}
	}

	l.Info().Log("msg", "db auto-migration done.")
	return nil
}

// Drop receives tables arguments and drop them in database.
func Drop(l log.Logger, db *gorm.DB, tables ...interface{}) error {
	l.Info().Log("msg", "running db drop")
	// We need to iterate throught the list in reverse order of
	// creation, since later tables may have constraints or
	// dependencies on earlier tables.
	len := len(tables)
	for i := range tables {
		table := tables[len-i-1]
		l.Debug().Log(
			"msg", fmt.Sprintf("drop table %T", table),
			"table", fmt.Sprintf("%T", table),
		)

		if err := db.DropTableIfExists(table).Error; err != nil {
			l.Crit().Log(
				"during", "db.DropTableIfExists",
				"err", err,
				"msg", fmt.Sprintf("error drop table: %v", err),
			)
			return err
		}
	}

	l.Info().Log("msg", "db drop complete")
	return nil
}

// Truncate receives tables arguments and truncate their content in database.
func Truncate(l log.Logger, db *gorm.DB, tables ...interface{}) error {
	l.Info().Log("msg", "running db truncate")
	// We need to iterate throught the list in reverse order of
	// creation, since later tables may have constraints or
	// dependencies on earlier tables.
	len := len(tables)
	for i := range tables {
		table := tables[len-i-1]
		l.Debug().Log(
			"msg", fmt.Sprintf("truncate table %T", table),
			"table", fmt.Sprintf("%T", table),
		)

		err := db.Exec(`TRUNCATE TABLE ` + db.NewScope(table).QuotedTableName()).Error
		if err != nil {
			l.Crit().Log(
				"during", "Truncate",
				"err", err,
				"msg", fmt.Sprintf("error truncate table: %v", err),
			)
			return err
		}
	}

	l.Info().Log("msg", "db truncate complete")
	return nil
}
