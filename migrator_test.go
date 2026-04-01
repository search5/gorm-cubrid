package cubrid

import (
	"database/sql"
	"database/sql/driver"
	"reflect"
	"testing"

	"gorm.io/gorm"
)

// newTestGormDB creates a gorm.DB backed by our custom mock driver.
func newTestGormDB(t *testing.T) (*gorm.DB, *mockDriver) {
	t.Helper()
	mockDB, md := newMockDriverDB()

	d := &Dialector{Config: &Config{Conn: mockDB}}
	db, err := gorm.Open(d, &gorm.Config{})
	if err != nil {
		t.Fatalf("gorm.Open: %v", err)
	}
	return db, md
}

// --- cubridColumnType tests ---

func TestCubridColumnType_AllMethods(t *testing.T) {
	ct := cubridColumnType{
		name:         "id",
		dataType:     "INTEGER",
		length:       10,
		hasLength:    true,
		precision:    10,
		scale:        2,
		hasDecimal:   true,
		nullable:     true,
		primaryKey:   true,
		unique:       true,
		autoInc:      true,
		defaultValue: sql.NullString{String: "0", Valid: true},
		comment:      "primary key",
	}

	if ct.Name() != "id" {
		t.Fatalf("Name: got %q", ct.Name())
	}
	if ct.DatabaseTypeName() != "INTEGER" {
		t.Fatalf("DatabaseTypeName: got %q", ct.DatabaseTypeName())
	}
	typ, ok := ct.ColumnType()
	if !ok || typ != "INTEGER" {
		t.Fatalf("ColumnType: got %q, %v", typ, ok)
	}
	pk, ok := ct.PrimaryKey()
	if !ok || !pk {
		t.Fatalf("PrimaryKey: got %v, %v", pk, ok)
	}
	ai, ok := ct.AutoIncrement()
	if !ok || !ai {
		t.Fatalf("AutoIncrement: got %v, %v", ai, ok)
	}
	l, ok := ct.Length()
	if !ok || l != 10 {
		t.Fatalf("Length: got %d, %v", l, ok)
	}
	p, s, ok := ct.DecimalSize()
	if !ok || p != 10 || s != 2 {
		t.Fatalf("DecimalSize: got %d, %d, %v", p, s, ok)
	}
	n, ok := ct.Nullable()
	if !ok || !n {
		t.Fatalf("Nullable: got %v, %v", n, ok)
	}
	u, ok := ct.Unique()
	if !ok || !u {
		t.Fatalf("Unique: got %v, %v", u, ok)
	}
	if ct.ScanType() != nil {
		t.Fatal("ScanType: expected nil")
	}
	c, ok := ct.Comment()
	if !ok || c != "primary key" {
		t.Fatalf("Comment: got %q, %v", c, ok)
	}
	dv, ok := ct.DefaultValue()
	if !ok || dv != "0" {
		t.Fatalf("DefaultValue: got %q, %v", dv, ok)
	}
}

func TestCubridColumnType_FalseValues(t *testing.T) {
	ct := cubridColumnType{
		name:         "col",
		dataType:     "VARCHAR",
		hasLength:    false,
		hasDecimal:   false,
		nullable:     false,
		primaryKey:   false,
		unique:       false,
		autoInc:      false,
		defaultValue: sql.NullString{Valid: false},
		comment:      "",
	}

	pk, _ := ct.PrimaryKey()
	if pk {
		t.Fatal("PrimaryKey: expected false")
	}
	ai, _ := ct.AutoIncrement()
	if ai {
		t.Fatal("AutoIncrement: expected false")
	}
	_, ok := ct.Length()
	if ok {
		t.Fatal("Length: expected ok=false")
	}
	_, _, ok = ct.DecimalSize()
	if ok {
		t.Fatal("DecimalSize: expected ok=false")
	}
	n, _ := ct.Nullable()
	if n {
		t.Fatal("Nullable: expected false")
	}
	u, _ := ct.Unique()
	if u {
		t.Fatal("Unique: expected false")
	}
	_, ok = ct.Comment()
	if ok {
		t.Fatal("Comment: expected ok=false for empty")
	}
	_, ok = ct.DefaultValue()
	if ok {
		t.Fatal("DefaultValue: expected ok=false")
	}
}

// --- cubridIndex tests ---

func TestCubridIndex_AllMethods(t *testing.T) {
	idx := &cubridIndex{
		table:      "users",
		name:       "idx_name",
		columns:    []string{"a", "b"},
		unique:     true,
		primaryKey: true,
	}

	if idx.Table() != "users" {
		t.Fatalf("Table: got %q", idx.Table())
	}
	if idx.Name() != "idx_name" {
		t.Fatalf("Name: got %q", idx.Name())
	}
	if !reflect.DeepEqual(idx.Columns(), []string{"a", "b"}) {
		t.Fatalf("Columns: got %v", idx.Columns())
	}
	pk, ok := idx.PrimaryKey()
	if !ok || !pk {
		t.Fatalf("PrimaryKey: got %v, %v", pk, ok)
	}
	u, ok := idx.Unique()
	if !ok || !u {
		t.Fatalf("Unique: got %v, %v", u, ok)
	}
	if idx.Option() != "" {
		t.Fatalf("Option: got %q", idx.Option())
	}
}

func TestCubridIndex_FalseValues(t *testing.T) {
	idx := &cubridIndex{unique: false, primaryKey: false}
	pk, _ := idx.PrimaryKey()
	if pk {
		t.Fatal("expected false")
	}
	u, _ := idx.Unique()
	if u {
		t.Fatal("expected false")
	}
}

// --- cubridTableType tests ---

func TestCubridTableType_AllMethods(t *testing.T) {
	tt := cubridTableType{
		schema:    "public",
		name:      "users",
		tableType: "CLASS",
		comment:   "user table",
	}

	if tt.Schema() != "public" {
		t.Fatalf("Schema: got %q", tt.Schema())
	}
	if tt.Name() != "users" {
		t.Fatalf("Name: got %q", tt.Name())
	}
	if tt.Type() != "CLASS" {
		t.Fatalf("Type: got %q", tt.Type())
	}
	c, ok := tt.Comment()
	if !ok || c != "user table" {
		t.Fatalf("Comment: got %q, %v", c, ok)
	}
}

func TestCubridTableType_EmptyComment(t *testing.T) {
	tt := cubridTableType{comment: ""}
	_, ok := tt.Comment()
	if ok {
		t.Fatal("expected ok=false for empty comment")
	}
}

// --- testModel for migrator tests ---

type testModel struct {
	ID   uint   `gorm:"primarykey"`
	Name string `gorm:"size:100"`
}

func (testModel) TableName() string { return "test_models" }

// --- Migrator method tests ---

func TestCurrentDatabase(t *testing.T) {
	db, md := newTestGormDB(t)

	md.pushResult([]string{"database()"}, [][]driver.Value{{"demodb"}})

	m := db.Migrator().(CubridMigrator)
	name := m.CurrentDatabase()
	if name != "demodb" {
		t.Fatalf("got %q, want %q", name, "demodb")
	}
}

func TestGetTables(t *testing.T) {
	db, md := newTestGormDB(t)

	md.pushResult([]string{"class_name"}, [][]driver.Value{
		{"users"},
		{"posts"},
	})

	m := db.Migrator().(CubridMigrator)
	tables, err := m.GetTables()
	if err != nil {
		t.Fatalf("GetTables: %v", err)
	}
	if len(tables) != 2 || tables[0] != "users" || tables[1] != "posts" {
		t.Fatalf("got %v", tables)
	}
}

func TestHasTable_True(t *testing.T) {
	db, md := newTestGormDB(t)

	md.pushResult([]string{"count"}, [][]driver.Value{{int64(1)}})

	m := db.Migrator().(CubridMigrator)
	if !m.HasTable(&testModel{}) {
		t.Fatal("expected HasTable=true")
	}
	if !md.containsQuery("db_class") {
		t.Fatal("expected query against db_class")
	}
}

func TestHasTable_False(t *testing.T) {
	db, md := newTestGormDB(t)

	md.pushResult([]string{"count"}, [][]driver.Value{{int64(0)}})

	m := db.Migrator().(CubridMigrator)
	if m.HasTable(&testModel{}) {
		t.Fatal("expected HasTable=false")
	}
}

func TestDropTable(t *testing.T) {
	db, md := newTestGormDB(t)

	m := db.Migrator().(CubridMigrator)
	err := m.DropTable(&testModel{})
	if err != nil {
		t.Fatalf("DropTable: %v", err)
	}
	if !md.containsQuery("DROP TABLE") {
		t.Fatal("expected DROP TABLE query")
	}
}

func TestRenameTable(t *testing.T) {
	db, md := newTestGormDB(t)

	m := db.Migrator().(CubridMigrator)
	err := m.RenameTable("old_table", "new_table")
	if err != nil {
		t.Fatalf("RenameTable: %v", err)
	}
	if !md.containsQuery("RENAME TABLE") {
		t.Fatal("expected RENAME TABLE query")
	}
}

func TestHasColumn_True(t *testing.T) {
	db, md := newTestGormDB(t)

	md.pushResult([]string{"count"}, [][]driver.Value{{int64(1)}})

	m := db.Migrator().(CubridMigrator)
	if !m.HasColumn(&testModel{}, "name") {
		t.Fatal("expected HasColumn=true")
	}
	if !md.containsQuery("db_attribute") {
		t.Fatal("expected query against db_attribute")
	}
}

func TestHasColumn_False(t *testing.T) {
	db, md := newTestGormDB(t)

	md.pushResult([]string{"count"}, [][]driver.Value{{int64(0)}})

	m := db.Migrator().(CubridMigrator)
	if m.HasColumn(&testModel{}, "email") {
		t.Fatal("expected HasColumn=false")
	}
}

func TestAlterColumn(t *testing.T) {
	db, md := newTestGormDB(t)

	m := db.Migrator().(CubridMigrator)
	err := m.AlterColumn(&testModel{}, "Name")
	if err != nil {
		t.Fatalf("AlterColumn: %v", err)
	}
	if !md.containsQuery("ALTER TABLE") {
		t.Fatal("expected ALTER TABLE query")
	}
}

func TestAlterColumn_NotFound(t *testing.T) {
	db, _ := newTestGormDB(t)

	m := db.Migrator().(CubridMigrator)
	err := m.AlterColumn(&testModel{}, "nonexistent_field")
	if err == nil {
		t.Fatal("expected error for nonexistent field")
	}
}

func TestRenameColumn(t *testing.T) {
	db, md := newTestGormDB(t)

	m := db.Migrator().(CubridMigrator)
	err := m.RenameColumn(&testModel{}, "Name", "Name")
	if err != nil {
		t.Fatalf("RenameColumn: %v", err)
	}
	if !md.containsQuery("RENAME COLUMN") {
		t.Fatal("expected RENAME COLUMN query")
	}
}

func TestRenameColumn_NotFound(t *testing.T) {
	db, _ := newTestGormDB(t)

	m := db.Migrator().(CubridMigrator)
	err := m.RenameColumn(&testModel{}, "nonexistent", "also_nonexistent")
	if err == nil {
		t.Fatal("expected error for nonexistent field")
	}
}

func TestHasIndex_True(t *testing.T) {
	db, md := newTestGormDB(t)

	md.pushResult([]string{"count"}, [][]driver.Value{{int64(1)}})

	m := db.Migrator().(CubridMigrator)
	if !m.HasIndex(&testModel{}, "idx_name") {
		t.Fatal("expected HasIndex=true")
	}
	if !md.containsQuery("db_index") {
		t.Fatal("expected query against db_index")
	}
}

func TestHasConstraint_True(t *testing.T) {
	db, md := newTestGormDB(t)

	md.pushResult([]string{"count"}, [][]driver.Value{{int64(1)}})

	m := db.Migrator().(CubridMigrator)
	if !m.HasConstraint(&testModel{}, "pk_id") {
		t.Fatal("expected HasConstraint=true")
	}
}

func TestRenameIndex(t *testing.T) {
	db, md := newTestGormDB(t)

	m := db.Migrator().(CubridMigrator)
	err := m.RenameIndex(&testModel{}, "old_idx", "new_idx")
	if err != nil {
		t.Fatalf("RenameIndex: %v", err)
	}
	if !md.containsQuery("RENAME INDEX") {
		t.Fatal("expected RENAME INDEX query")
	}
}

func TestColumnTypes(t *testing.T) {
	db, md := newTestGormDB(t)

	// Column info query result.
	md.pushResult(
		[]string{"attr_name", "data_type", "prec", "scale", "is_nullable", "default_value", "attr_type"},
		[][]driver.Value{
			{"id", "INTEGER", int64(10), int64(0), "NO", nil, "INSTANCE"},
			{"name", "VARCHAR", int64(100), int64(0), "YES", "hello", "INSTANCE"},
		},
	)
	// Primary key check for "id".
	md.pushResult([]string{"count"}, [][]driver.Value{{int64(1)}})
	// Unique check for "id".
	md.pushResult([]string{"count"}, [][]driver.Value{{int64(1)}})
	// Primary key check for "name".
	md.pushResult([]string{"count"}, [][]driver.Value{{int64(0)}})
	// Unique check for "name".
	md.pushResult([]string{"count"}, [][]driver.Value{{int64(0)}})

	m := db.Migrator().(CubridMigrator)
	cols, err := m.ColumnTypes(&testModel{})
	if err != nil {
		t.Fatalf("ColumnTypes: %v", err)
	}
	if len(cols) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(cols))
	}

	col0 := cols[0].(cubridColumnType)
	if col0.Name() != "id" {
		t.Fatalf("col[0] name: got %q", col0.Name())
	}
	if col0.DatabaseTypeName() != "INTEGER" {
		t.Fatalf("col[0] type: got %q", col0.DatabaseTypeName())
	}
	pk, _ := col0.PrimaryKey()
	if !pk {
		t.Fatal("col[0] expected primary key")
	}
	n, _ := col0.Nullable()
	if n {
		t.Fatal("col[0] should not be nullable")
	}

	col1 := cols[1].(cubridColumnType)
	if col1.Name() != "name" {
		t.Fatalf("col[1] name: got %q", col1.Name())
	}
	n2, _ := col1.Nullable()
	if !n2 {
		t.Fatal("col[1] should be nullable")
	}
	dv, dvOk := col1.DefaultValue()
	if !dvOk || dv != "hello" {
		t.Fatalf("col[1] default: got %q, %v", dv, dvOk)
	}
}

func TestColumnTypes_WithDecimal(t *testing.T) {
	db, md := newTestGormDB(t)

	md.pushResult(
		[]string{"attr_name", "data_type", "prec", "scale", "is_nullable", "default_value", "attr_type"},
		[][]driver.Value{
			{"price", "NUMERIC", int64(10), int64(2), "YES", nil, "INSTANCE"},
		},
	)
	md.pushResult([]string{"count"}, [][]driver.Value{{int64(0)}})
	md.pushResult([]string{"count"}, [][]driver.Value{{int64(0)}})

	m := db.Migrator().(CubridMigrator)
	cols, err := m.ColumnTypes(&testModel{})
	if err != nil {
		t.Fatalf("ColumnTypes: %v", err)
	}
	if len(cols) != 1 {
		t.Fatalf("expected 1 column, got %d", len(cols))
	}

	col := cols[0].(cubridColumnType)
	p, s, ok := col.DecimalSize()
	if !ok || p != 10 || s != 2 {
		t.Fatalf("DecimalSize: got %d, %d, %v", p, s, ok)
	}
}

func TestGetIndexes(t *testing.T) {
	db, md := newTestGormDB(t)

	md.pushResult(
		[]string{"index_name", "is_unique", "is_primary_key", "key_attr_name"},
		[][]driver.Value{
			{"pk_id", "YES", "YES", "id"},
			{"idx_name", "NO", "NO", "name"},
			{"idx_composite", "YES", "NO", "name"},
			{"idx_composite", "YES", "NO", "id"},
		},
	)

	m := db.Migrator().(CubridMigrator)
	indexes, err := m.GetIndexes(&testModel{})
	if err != nil {
		t.Fatalf("GetIndexes: %v", err)
	}
	if len(indexes) != 3 {
		t.Fatalf("expected 3 indexes, got %d", len(indexes))
	}

	for _, idx := range indexes {
		if idx.Name() == "idx_composite" {
			cols := idx.Columns()
			if len(cols) != 2 {
				t.Fatalf("idx_composite: expected 2 columns, got %d", len(cols))
			}
		}
	}
}

func TestTableType_Method(t *testing.T) {
	db, md := newTestGormDB(t)

	md.pushResult([]string{"class_type"}, [][]driver.Value{{"CLASS"}})

	m := db.Migrator().(CubridMigrator)
	tt, err := m.TableType(&testModel{})
	if err != nil {
		t.Fatalf("TableType: %v", err)
	}
	if tt.Name() != "test_models" {
		t.Fatalf("name: got %q", tt.Name())
	}
	if tt.Type() != "CLASS" {
		t.Fatalf("type: got %q", tt.Type())
	}
}

func TestCreateView_Method(t *testing.T) {
	db, md := newTestGormDB(t)

	m := db.Migrator().(CubridMigrator)
	stmt := &gorm.Statement{}
	stmt.SQL.WriteString("SELECT * FROM users")
	err := m.CreateView("my_view", gorm.ViewOption{
		Query: &gorm.DB{Statement: stmt},
	})
	if err != nil {
		t.Fatalf("CreateView: %v", err)
	}
	if !md.containsQuery("CREATE VIEW") {
		t.Fatal("expected CREATE VIEW query")
	}
}

func TestCreateView_Replace(t *testing.T) {
	db, md := newTestGormDB(t)

	m := db.Migrator().(CubridMigrator)
	stmt := &gorm.Statement{}
	stmt.SQL.WriteString("SELECT * FROM users")
	err := m.CreateView("my_view", gorm.ViewOption{
		Replace: true,
		Query:   &gorm.DB{Statement: stmt},
	})
	if err != nil {
		t.Fatalf("CreateView replace: %v", err)
	}
	if !md.containsQuery("CREATE OR REPLACE VIEW") {
		t.Fatal("expected CREATE OR REPLACE VIEW query")
	}
}

func TestDropView_Method(t *testing.T) {
	db, md := newTestGormDB(t)

	m := db.Migrator().(CubridMigrator)
	err := m.DropView("my_view")
	if err != nil {
		t.Fatalf("DropView: %v", err)
	}
	if !md.containsQuery("DROP VIEW") {
		t.Fatal("expected DROP VIEW query")
	}
}

func TestCreateTable_Method(t *testing.T) {
	db, md := newTestGormDB(t)

	m := db.Migrator().(CubridMigrator)
	// CreateTable exercises the full code path including SQL generation.
	_ = m.CreateTable(&testModel{})
	if !md.containsQuery("CREATE TABLE") {
		t.Fatal("expected CREATE TABLE query")
	}
}
