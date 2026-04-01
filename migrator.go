package cubrid

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/migrator"
	"gorm.io/gorm/schema"
)

// CubridMigrator extends the base GORM migrator with CUBRID-specific behavior.
type CubridMigrator struct {
	migrator.Migrator
	Dialector Dialector
}

// CurrentDatabase returns the name of the current database.
func (m CubridMigrator) CurrentDatabase() string {
	var name string
	m.DB.Raw("SELECT database()").Row().Scan(&name)
	return name
}

// GetTables returns all user table names.
func (m CubridMigrator) GetTables() (tables []string, err error) {
	err = m.DB.Raw(
		"SELECT class_name FROM db_class WHERE is_system_class = 'NO' AND class_type = 'CLASS' ORDER BY class_name",
	).Scan(&tables).Error
	return
}

// CreateTable creates a table for the given models.
func (m CubridMigrator) CreateTable(values ...interface{}) error {
	for _, value := range m.ReorderModels(values, false) {
		tx := m.DB.Session(&gorm.Session{})
		if err := m.RunWithValue(value, func(stmt *gorm.Statement) (err error) {
			var (
				createTableSQL          = "CREATE TABLE ? ("
				args                    = []interface{}{m.CurrentTable(stmt)}
				hasPrimaryKeyInDataType bool
			)

			for _, dbName := range stmt.Schema.DBNames {
				field := stmt.Schema.FieldsByDBName[dbName]
				if !field.IgnoreMigration {
					createTableSQL += "? ?"
					hasPrimaryKeyInDataType = hasPrimaryKeyInDataType || strings.Contains(strings.ToUpper(string(field.DataType)), "PRIMARY KEY")
					args = append(args, clause.Column{Name: dbName}, m.FullDataTypeOf(field))
					createTableSQL += ","
				}
			}

			if !hasPrimaryKeyInDataType && len(stmt.Schema.PrimaryFields) > 0 {
				createTableSQL += "PRIMARY KEY ?,"
				primaryKeys := make([]interface{}, 0, len(stmt.Schema.PrimaryFields))
				for _, field := range stmt.Schema.PrimaryFields {
					primaryKeys = append(primaryKeys, clause.Column{Name: field.DBName})
				}
				args = append(args, primaryKeys)
			}

			for _, idx := range stmt.Schema.ParseIndexes() {
				if m.CreateIndexAfterCreateTable {
					continue
				}
				if idx.Class != "" {
					createTableSQL += idx.Class + " "
				}
				createTableSQL += "INDEX ? (?)"
				if idx.Comment != "" {
					createTableSQL += fmt.Sprintf(" COMMENT '%s'", idx.Comment)
				}
				args = append(args, clause.Column{Name: idx.Name}, m.indexColumns(idx))
				createTableSQL += ","
			}

			createTableSQL = strings.TrimSuffix(createTableSQL, ",")
			createTableSQL += ")"

			return tx.Exec(createTableSQL, args...).Error
		}); err != nil {
			return err
		}
	}
	return nil
}

// HasTable checks if a table exists.
func (m CubridMigrator) HasTable(value interface{}) bool {
	var count int64
	m.RunWithValue(value, func(stmt *gorm.Statement) error {
		return m.DB.Raw(
			"SELECT COUNT(*) FROM db_class WHERE class_name = ? AND is_system_class = 'NO'",
			stmt.Table,
		).Row().Scan(&count)
	})
	return count > 0
}

// DropTable drops the given tables.
func (m CubridMigrator) DropTable(values ...interface{}) error {
	for i := len(values) - 1; i >= 0; i-- {
		tx := m.DB.Session(&gorm.Session{})
		if err := m.RunWithValue(values[i], func(stmt *gorm.Statement) error {
			return tx.Exec("DROP TABLE IF EXISTS ?", m.CurrentTable(stmt)).Error
		}); err != nil {
			return err
		}
	}
	return nil
}

// RenameTable renames a table.
func (m CubridMigrator) RenameTable(oldName, newName interface{}) error {
	return m.DB.Exec("RENAME TABLE ? TO ?",
		clause.Table{Name: fmt.Sprint(oldName)},
		clause.Table{Name: fmt.Sprint(newName)},
	).Error
}

// HasColumn checks if a column exists in a table.
func (m CubridMigrator) HasColumn(value interface{}, field string) bool {
	var count int64
	m.RunWithValue(value, func(stmt *gorm.Statement) error {
		return m.DB.Raw(
			"SELECT COUNT(*) FROM db_attribute WHERE class_name = ? AND attr_name = ?",
			stmt.Table, field,
		).Row().Scan(&count)
	})
	return count > 0
}

// AlterColumn modifies a column definition.
func (m CubridMigrator) AlterColumn(value interface{}, field string) error {
	return m.RunWithValue(value, func(stmt *gorm.Statement) error {
		if f := stmt.Schema.LookUpField(field); f != nil {
			fileType := m.FullDataTypeOf(f)
			return m.DB.Exec("ALTER TABLE ? MODIFY COLUMN ? ?",
				m.CurrentTable(stmt),
				clause.Column{Name: f.DBName},
				fileType,
			).Error
		}
		return fmt.Errorf("failed to look up field %q", field)
	})
}

// RenameColumn renames a column.
func (m CubridMigrator) RenameColumn(value interface{}, oldName, newName string) error {
	return m.RunWithValue(value, func(stmt *gorm.Statement) error {
		var f *schema.Field
		if f = stmt.Schema.LookUpField(newName); f == nil {
			if f = stmt.Schema.LookUpField(oldName); f == nil {
				return fmt.Errorf("failed to look up field %q", oldName)
			}
		}
		return m.DB.Exec("ALTER TABLE ? RENAME COLUMN ? AS ?",
			m.CurrentTable(stmt),
			clause.Column{Name: oldName},
			clause.Column{Name: f.DBName},
		).Error
	})
}

// ColumnTypes returns column type information for a table.
func (m CubridMigrator) ColumnTypes(value interface{}) ([]gorm.ColumnType, error) {
	columnTypes := make([]gorm.ColumnType, 0)
	err := m.RunWithValue(value, func(stmt *gorm.Statement) error {
		rows, err := m.DB.Session(&gorm.Session{}).Raw(
			"SELECT attr_name, data_type, prec, scale, is_nullable, default_value, "+
				"attr_type FROM db_attribute WHERE class_name = ? ORDER BY def_order",
			stmt.Table,
		).Rows()
		if err != nil {
			return err
		}
		defer rows.Close()

		for rows.Next() {
			var (
				name         string
				dataType     string
				precision    sql.NullInt64
				scale        sql.NullInt64
				nullable     string
				defaultValue sql.NullString
				attrType     string
			)
			if err := rows.Scan(&name, &dataType, &precision, &scale, &nullable, &defaultValue, &attrType); err != nil {
				return err
			}

			ct := cubridColumnType{
				name:         strings.TrimSpace(name),
				dataType:     strings.TrimSpace(dataType),
				nullable:     nullable == "YES",
				defaultValue: defaultValue,
			}
			if precision.Valid {
				ct.length = precision.Int64
				ct.hasLength = true
			}
			if scale.Valid {
				ct.precision = precision.Int64
				ct.scale = scale.Int64
				ct.hasDecimal = scale.Int64 > 0
			}

			// Check primary key.
			var pkCount int64
			m.DB.Raw(
				"SELECT COUNT(*) FROM db_index i, db_index_key k "+
					"WHERE i.class_name = ? AND i.is_primary_key = 'YES' "+
					"AND k.class_name = i.class_name AND k.index_name = i.index_name "+
					"AND k.key_attr_name = ?",
				stmt.Table, ct.name,
			).Row().Scan(&pkCount)
			ct.primaryKey = pkCount > 0

			// Check unique.
			var uqCount int64
			m.DB.Raw(
				"SELECT COUNT(*) FROM db_index i, db_index_key k "+
					"WHERE i.class_name = ? AND i.is_unique = 'YES' "+
					"AND k.class_name = i.class_name AND k.index_name = i.index_name "+
					"AND k.key_attr_name = ?",
				stmt.Table, ct.name,
			).Row().Scan(&uqCount)
			ct.unique = uqCount > 0

			columnTypes = append(columnTypes, ct)
		}
		return nil
	})
	return columnTypes, err
}

// HasIndex checks if an index exists.
func (m CubridMigrator) HasIndex(value interface{}, name string) bool {
	var count int64
	m.RunWithValue(value, func(stmt *gorm.Statement) error {
		return m.DB.Raw(
			"SELECT COUNT(*) FROM db_index WHERE class_name = ? AND index_name = ?",
			stmt.Table, name,
		).Row().Scan(&count)
	})
	return count > 0
}

// RenameIndex renames an index (CUBRID doesn't support direct rename, so drop+create).
func (m CubridMigrator) RenameIndex(value interface{}, oldName, newName string) error {
	return m.RunWithValue(value, func(stmt *gorm.Statement) error {
		return m.DB.Exec("ALTER TABLE ? RENAME INDEX ? AS ?",
			m.CurrentTable(stmt),
			clause.Column{Name: oldName},
			clause.Column{Name: newName},
		).Error
	})
}

// HasConstraint checks if a constraint exists.
func (m CubridMigrator) HasConstraint(value interface{}, name string) bool {
	var count int64
	m.RunWithValue(value, func(stmt *gorm.Statement) error {
		return m.DB.Raw(
			"SELECT COUNT(*) FROM db_index WHERE class_name = ? AND index_name = ?",
			stmt.Table, name,
		).Row().Scan(&count)
	})
	return count > 0
}

// GetIndexes returns all indexes for a table.
func (m CubridMigrator) GetIndexes(value interface{}) ([]gorm.Index, error) {
	var indexes []gorm.Index
	err := m.RunWithValue(value, func(stmt *gorm.Statement) error {
		rows, err := m.DB.Raw(
			"SELECT i.index_name, i.is_unique, i.is_primary_key, k.key_attr_name "+
				"FROM db_index i, db_index_key k "+
				"WHERE i.class_name = ? AND k.class_name = i.class_name AND k.index_name = i.index_name "+
				"ORDER BY i.index_name, k.key_order",
			stmt.Table,
		).Rows()
		if err != nil {
			return err
		}
		defer rows.Close()

		indexMap := make(map[string]*cubridIndex)
		for rows.Next() {
			var name, isUnique, isPK, colName string
			if err := rows.Scan(&name, &isUnique, &isPK, &colName); err != nil {
				return err
			}
			name = strings.TrimSpace(name)
			colName = strings.TrimSpace(colName)

			if idx, ok := indexMap[name]; ok {
				idx.columns = append(idx.columns, colName)
			} else {
				indexMap[name] = &cubridIndex{
					table:      stmt.Table,
					name:       name,
					columns:    []string{colName},
					unique:     isUnique == "YES",
					primaryKey: isPK == "YES",
				}
			}
		}

		for _, idx := range indexMap {
			indexes = append(indexes, idx)
		}
		return nil
	})
	return indexes, err
}

// TableType returns type information for a table.
func (m CubridMigrator) TableType(value interface{}) (gorm.TableType, error) {
	var tt cubridTableType
	err := m.RunWithValue(value, func(stmt *gorm.Statement) error {
		tt.name = stmt.Table
		var classType string
		err := m.DB.Raw(
			"SELECT class_type FROM db_class WHERE class_name = ?",
			stmt.Table,
		).Row().Scan(&classType)
		if err != nil {
			return err
		}
		tt.tableType = strings.TrimSpace(classType)
		return nil
	})
	return tt, err
}

// CreateView creates a view.
func (m CubridMigrator) CreateView(name string, option gorm.ViewOption) error {
	sql := "CREATE "
	if option.Replace {
		sql = "CREATE OR REPLACE "
	}
	sql += "VIEW " + name + " AS " + option.Query.Statement.SQL.String()
	return m.DB.Exec(sql, option.Query.Statement.Vars...).Error
}

// DropView drops a view.
func (m CubridMigrator) DropView(name string) error {
	return m.DB.Exec("DROP VIEW IF EXISTS " + name).Error
}

func (m CubridMigrator) indexColumns(idx *schema.Index) []interface{} {
	columns := make([]interface{}, len(idx.Fields))
	for i, field := range idx.Fields {
		columns[i] = clause.Column{Name: field.DBName}
	}
	return columns
}

// --- cubridColumnType implements gorm.ColumnType ---

type cubridColumnType struct {
	name         string
	dataType     string
	length       int64
	hasLength    bool
	precision    int64
	scale        int64
	hasDecimal   bool
	nullable     bool
	primaryKey   bool
	unique       bool
	autoInc      bool
	defaultValue sql.NullString
	comment      string
}

func (c cubridColumnType) Name() string                           { return c.name }
func (c cubridColumnType) DatabaseTypeName() string               { return c.dataType }
func (c cubridColumnType) ColumnType() (string, bool)             { return c.dataType, true }
func (c cubridColumnType) PrimaryKey() (bool, bool)               { return c.primaryKey, true }
func (c cubridColumnType) AutoIncrement() (bool, bool)            { return c.autoInc, true }
func (c cubridColumnType) Length() (int64, bool)                  { return c.length, c.hasLength }
func (c cubridColumnType) DecimalSize() (int64, int64, bool)      { return c.precision, c.scale, c.hasDecimal }
func (c cubridColumnType) Nullable() (bool, bool)                 { return c.nullable, true }
func (c cubridColumnType) Unique() (bool, bool)                   { return c.unique, true }
func (c cubridColumnType) ScanType() reflect.Type                 { return nil }
func (c cubridColumnType) Comment() (string, bool)                { return c.comment, c.comment != "" }
func (c cubridColumnType) DefaultValue() (string, bool)           { return c.defaultValue.String, c.defaultValue.Valid }

// --- cubridIndex implements gorm.Index ---

type cubridIndex struct {
	table      string
	name       string
	columns    []string
	unique     bool
	primaryKey bool
}

func (i *cubridIndex) Table() string                     { return i.table }
func (i *cubridIndex) Name() string                      { return i.name }
func (i *cubridIndex) Columns() []string                 { return i.columns }
func (i *cubridIndex) PrimaryKey() (bool, bool)          { return i.primaryKey, true }
func (i *cubridIndex) Unique() (bool, bool)              { return i.unique, true }
func (i *cubridIndex) Option() string                    { return "" }

// --- cubridTableType implements gorm.TableType ---

type cubridTableType struct {
	schema    string
	name      string
	tableType string
	comment   string
}

func (t cubridTableType) Schema() string            { return t.schema }
func (t cubridTableType) Name() string              { return t.name }
func (t cubridTableType) Type() string              { return t.tableType }
func (t cubridTableType) Comment() (string, bool)   { return t.comment, t.comment != "" }
