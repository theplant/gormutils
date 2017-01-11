// Package dbutils provides a list of useful functions for operating database.
package gormutils

import (
	"fmt"
	"log"
	"regexp"
	"runtime/debug"

	"github.com/jinzhu/gorm"
)

// keyMatcher is for extracting 'id' out of eg. 'users (id)', to
// convert the definition of the constraing column to the format used
// in the constraint name.
var keyMatcher = regexp.MustCompile(" ?\\(([^)]+)\\)?")

// EnsureForeignKey will attempt to add the given foreign-key
// constraint to the database, and swallow any error that is triggered
// if the constraint already exists. The intent is to be an idempotent
// function that is safe to be called multiple times.
func EnsureForeignKey(db *gorm.DB, model interface{}, sourceTable string, column string, table string, onDelete string, onUpdate string) error {
	scope := db.NewScope(model) // Mainly for logging
	err := db.Model(model).AddForeignKey(column, table, onDelete, onUpdate).Error
	if err != nil {
		tableFragment := keyMatcher.ReplaceAllString(table, "_${1}")
		expectedMsg := fmt.Sprintf("pq: constraint \"%s_%s_%s_foreign\" for relation \"%s\" already exists", sourceTable, column, tableFragment, sourceTable)

		if err.Error() == expectedMsg {
			scope.Log(fmt.Sprintf("Foreign-key constraint appears to already exist: %s", err.Error()))
		} else {
			return err
		}
	}
	return nil
}

// EnsureConstraint will attempt to add the given table constraint
// to the database, and swallow any error that is triggered if the
// constraint already exists. The intent is to be an idempotent
// function that is safe to be called multiple times, similar to
// EnsureForeignKey.
func EnsureConstraint(db *gorm.DB, model interface{}, name string, constraint string) error {
	scope := db.NewScope(model)
	query := `ALTER TABLE %s ADD CONSTRAINT %s %s;`

	err := db.Exec(fmt.Sprintf(query, scope.QuotedTableName(), scope.Quote(name), constraint)).Error
	if err != nil {
		expectedMsg := fmt.Sprintf("pq: constraint \"%s\" for relation \"%s\" already exists", name, scope.TableName())

		if err.Error() == expectedMsg {
			scope.Log(fmt.Sprintf("Constraint appears to already exist: %s", err.Error()))
		} else {
			return err
		}
	}
	return nil
}

// EnsureIndex will attempt to create the index for the given
// table, and swallow any error that is triggered if the index
// already exists. The intent is to be an idempotent function
// that is safe to be called multiple times, similar to
// EnsureForeignKey.
//
// Eventually it would be better to switch to Postgres 9.5 and
// use `CREATE INDEX IF NOT EXISTS ...`, then this function can
// be removed.
func EnsureIndex(db *gorm.DB, model interface{}, name string, constraint string, unique bool) error {
	scope := db.NewScope(model)
	query := `CREATE INDEX %s ON %s;`

	if unique {
		query = `CREATE UNIQUE INDEX %s ON %s;`
	}

	err := db.Exec(fmt.Sprintf(query, scope.Quote(name), constraint)).Error
	if err != nil {

		expectedMsg := fmt.Sprintf("pq: relation \"%s\" already exists", name)

		if err.Error() == expectedMsg {
			scope.Log(fmt.Sprintf("Index appears to already exist: %s", err.Error()))
		} else {
			return err
		}
	}
	return nil
}

func Transact(db *gorm.DB, f func(tx *gorm.DB) error) (err error) {
	tx := db.Begin()
	defer func() {
		if r := recover(); r != nil {
			if er, ok := r.(error); ok {
				err = er
			} else {
				err = fmt.Errorf("%+v", r)
			}
			log.Println(err)
			debug.PrintStack()
		}

		if err != nil {
			if e := tx.Rollback().Error; e != nil {
				log.Println("Rollback Error:", e)
			}
		} else {
			err = tx.Commit().Error
		}
	}()

	err = f(tx)

	return
}

func ToUpdateColumnsMap(db *gorm.DB, prefix string, structval interface{}) (r map[string]interface{}) {
	fs := db.NewScope(structval).Fields()
	r = make(map[string]interface{})
	for _, f := range fs {
		r[prefix+f.DBName] = f.Field.Interface()
	}
	return
}
