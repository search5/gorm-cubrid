// Package cubrid provides a GORM dialector for the CUBRID database.
//
// Usage:
//
//	import (
//	    "gorm.io/gorm"
//	    cubrid "github.com/search5/gorm-cubrid"
//	)
//
//	db, err := gorm.Open(cubrid.Open("cubrid://dba:@localhost:33000/demodb"), &gorm.Config{})
package cubrid

import (
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/search5/cubrid-go"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/migrator"
	"gorm.io/gorm/schema"
)

// Config holds CUBRID-specific dialector configuration.
type Config struct {
	// DSN is the CUBRID connection string.
	// Format: cubrid://user:password@host:port/dbname?params
	DSN string

	// Conn is an existing database connection to reuse.
	Conn *sql.DB

	// DriverName is the registered driver name. Defaults to "cubrid".
	DriverName string

	// DefaultStringSize is the default size for VARCHAR columns. Defaults to 255.
	DefaultStringSize uint
}

// Dialector implements gorm.Dialector for CUBRID.
type Dialector struct {
	*Config
}

// Open creates a CUBRID dialector from a DSN string.
func Open(dsn string) gorm.Dialector {
	return &Dialector{Config: &Config{DSN: dsn}}
}

// New creates a CUBRID dialector from a Config.
func New(config Config) gorm.Dialector {
	return &Dialector{Config: &config}
}

// Name returns the dialect name.
func (d Dialector) Name() string {
	return "cubrid"
}

// Initialize sets up the database connection and registers clause builders.
func (d Dialector) Initialize(db *gorm.DB) error {
	if d.DriverName == "" {
		d.DriverName = "cubrid"
	}
	if d.DefaultStringSize == 0 {
		d.DefaultStringSize = 255
	}

	if d.Conn != nil {
		db.ConnPool = d.Conn
	} else {
		conn, err := sql.Open(d.DriverName, d.DSN)
		if err != nil {
			return err
		}
		db.ConnPool = conn
	}

	// Register clause builders for CUBRID-specific SQL generation.
	for k, v := range d.ClauseBuilders() {
		db.ClauseBuilders[k] = v
	}

	return nil
}

// ClauseBuilders returns CUBRID-specific clause builders.
func (d Dialector) ClauseBuilders() map[string]clause.ClauseBuilder {
	return map[string]clause.ClauseBuilder{}
}

// Migrator returns a CUBRID migrator.
func (d Dialector) Migrator(db *gorm.DB) gorm.Migrator {
	return CubridMigrator{
		Migrator: migrator.Migrator{
			Config: migrator.Config{
				DB:                          db,
				Dialector:                   d,
				CreateIndexAfterCreateTable: true,
			},
		},
		Dialector: d,
	}
}

// DataTypeOf maps a schema field to a CUBRID SQL type string.
func (d Dialector) DataTypeOf(field *schema.Field) string {
	switch field.DataType {
	case schema.Bool:
		return "SHORT"

	case schema.Int, schema.Uint:
		return d.intDataType(field)

	case schema.Float:
		if field.Precision > 0 {
			return fmt.Sprintf("NUMERIC(%d,%d)", field.Precision, field.Scale)
		}
		if field.Size <= 32 {
			return "FLOAT"
		}
		return "DOUBLE"

	case schema.String:
		size := field.Size
		if size == 0 {
			if d.DefaultStringSize > 0 {
				size = int(d.DefaultStringSize)
			} else {
				size = 255
			}
		}
		if size >= 1073741823 {
			return "STRING"
		}
		return fmt.Sprintf("VARCHAR(%d)", size)

	case schema.Bytes:
		if field.Size > 0 && field.Size < 1073741823 {
			return fmt.Sprintf("VARBIT(%d)", field.Size*8)
		}
		return "BLOB"

	case schema.Time:
		return "DATETIME"

	default:
		return string(field.DataType)
	}
}

// intDataType returns the integer SQL type for a field.
func (d Dialector) intDataType(field *schema.Field) string {
	constraint := func(prefix string) string {
		if field.AutoIncrement {
			return prefix + " AUTO_INCREMENT"
		}
		return prefix
	}

	switch {
	case field.DataType == schema.Uint:
		switch {
		case field.Size <= 16:
			return constraint("INT")
		case field.Size <= 32:
			return constraint("BIGINT")
		default:
			return constraint("BIGINT")
		}
	default: // Int
		switch {
		case field.Size <= 16:
			return constraint("SHORT")
		case field.Size <= 32:
			return constraint("INT")
		default:
			return constraint("BIGINT")
		}
	}
}

// DefaultValueOf returns the default value expression for a field.
func (d Dialector) DefaultValueOf(field *schema.Field) clause.Expression {
	return clause.Expr{SQL: "DEFAULT NULL"}
}

// BindVarTo writes a bind variable placeholder.
func (d Dialector) BindVarTo(writer clause.Writer, stmt *gorm.Statement, v interface{}) {
	writer.WriteByte('?')
}

// QuoteTo quotes an identifier for CUBRID (uses double quotes).
func (d Dialector) QuoteTo(writer clause.Writer, str string) {
	writer.WriteByte('"')
	if strings.Contains(str, ".") {
		parts := strings.Split(str, ".")
		for i, p := range parts {
			if i > 0 {
				writer.WriteString(`"."`)
			}
			writer.WriteString(p)
		}
	} else {
		writer.WriteString(str)
	}
	writer.WriteByte('"')
}

// Explain formats a SQL statement with bound variables for logging.
func (d Dialector) Explain(sql string, vars ...interface{}) string {
	return logger(sql, vars...)
}

// SavePoint creates a transaction savepoint.
func (d Dialector) SavePoint(tx *gorm.DB, name string) error {
	return tx.Exec("SAVEPOINT " + name).Error
}

// RollbackTo rolls back to a transaction savepoint.
func (d Dialector) RollbackTo(tx *gorm.DB, name string) error {
	return tx.Exec("ROLLBACK TO SAVEPOINT " + name).Error
}

// logger formats SQL with variables for explain output.
func logger(sql string, vars ...interface{}) string {
	for _, v := range vars {
		sql = strings.Replace(sql, "?", fmt.Sprintf("'%v'", v), 1)
	}
	return sql
}

// Compile-time interface checks.
var (
	_ gorm.Dialector                    = (*Dialector)(nil)
	_ gorm.SavePointerDialectorInterface = (*Dialector)(nil)
)
