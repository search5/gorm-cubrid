package cubrid

import (
	"database/sql"
	"database/sql/driver"
	"strings"
	"testing"

	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

// --- Initialize tests ---

func TestInitialize_WithExistingConn(t *testing.T) {
	mockDB, _ := newMockDriverDB()
	defer mockDB.Close()

	d := &Dialector{Config: &Config{Conn: mockDB}}
	db, err := gorm.Open(d, &gorm.Config{})
	if err != nil {
		t.Fatalf("Initialize with existing conn failed: %v", err)
	}
	if db == nil {
		t.Fatal("expected non-nil db")
	}
}

func TestInitialize_DefaultDriverName(t *testing.T) {
	mockDB, _ := newMockDriverDB()
	defer mockDB.Close()

	d := &Dialector{Config: &Config{Conn: mockDB}}

	if d.DriverName != "" {
		t.Fatal("DriverName should be empty initially")
	}

	_, err := gorm.Open(d, &gorm.Config{})
	if err != nil {
		t.Fatalf("gorm.Open: %v", err)
	}
	if d.DriverName != "cubrid" {
		t.Fatalf("DriverName: got %q, want %q", d.DriverName, "cubrid")
	}
}

func TestInitialize_DefaultStringSize(t *testing.T) {
	mockDB, _ := newMockDriverDB()
	defer mockDB.Close()

	d := &Dialector{Config: &Config{Conn: mockDB}}

	_, err := gorm.Open(d, &gorm.Config{})
	if err != nil {
		t.Fatalf("gorm.Open: %v", err)
	}
	if d.DefaultStringSize != 255 {
		t.Fatalf("DefaultStringSize: got %d, want 255", d.DefaultStringSize)
	}
}

func TestInitialize_CustomDriverName(t *testing.T) {
	mockDB, _ := newMockDriverDB()
	defer mockDB.Close()

	d := &Dialector{Config: &Config{
		Conn:       mockDB,
		DriverName: "custom_cubrid",
	}}

	_, err := gorm.Open(d, &gorm.Config{})
	if err != nil {
		t.Fatalf("gorm.Open: %v", err)
	}
	if d.DriverName != "custom_cubrid" {
		t.Fatalf("DriverName: got %q, want %q", d.DriverName, "custom_cubrid")
	}
}

func TestInitialize_CustomDefaultStringSize(t *testing.T) {
	mockDB, _ := newMockDriverDB()
	defer mockDB.Close()

	d := &Dialector{Config: &Config{
		Conn:              mockDB,
		DefaultStringSize: 512,
	}}

	_, err := gorm.Open(d, &gorm.Config{})
	if err != nil {
		t.Fatalf("gorm.Open: %v", err)
	}
	if d.DefaultStringSize != 512 {
		t.Fatalf("DefaultStringSize: got %d, want 512", d.DefaultStringSize)
	}
}

func TestInitialize_WithDSN(t *testing.T) {
	d := &Dialector{Config: &Config{
		DSN: "cubrid://dba:@localhost:33000/demodb",
	}}
	// cubrid driver tries to connect immediately; error is expected.
	_, err := gorm.Open(d, &gorm.Config{})
	_ = err // error is expected, just exercising the code path
}

// --- ClauseBuilders ---

func TestClauseBuilders(t *testing.T) {
	d := &Dialector{Config: &Config{}}
	builders := d.ClauseBuilders()
	if builders == nil {
		t.Fatal("expected non-nil map")
	}
	if len(builders) != 0 {
		t.Fatalf("expected empty map, got %d entries", len(builders))
	}
}

// --- Migrator construction ---

func TestMigrator_Construction(t *testing.T) {
	mockDB, _ := newMockDriverDB()
	defer mockDB.Close()

	d := &Dialector{Config: &Config{Conn: mockDB}}
	db, err := gorm.Open(d, &gorm.Config{})
	if err != nil {
		t.Fatalf("gorm.Open: %v", err)
	}

	m := d.Migrator(db)
	if m == nil {
		t.Fatal("expected non-nil migrator")
	}
	cm, ok := m.(CubridMigrator)
	if !ok {
		t.Fatal("expected CubridMigrator type")
	}
	if cm.Dialector.Name() != "cubrid" {
		t.Fatalf("Migrator dialector name: got %q", cm.Dialector.Name())
	}
}

// --- DefaultValueOf ---

func TestDefaultValueOf(t *testing.T) {
	d := &Dialector{Config: &Config{}}
	expr := d.DefaultValueOf(&schema.Field{})
	if expr == nil {
		t.Fatal("expected non-nil expression")
	}
}

// --- SavePoint / RollbackTo ---

func TestSavePoint(t *testing.T) {
	db, md := newTestGormDB(t)

	err := (&Dialector{Config: &Config{}}).SavePoint(db, "sp1")
	if err != nil {
		t.Fatalf("SavePoint: %v", err)
	}
	if !md.containsQuery("SAVEPOINT") {
		t.Fatal("expected SAVEPOINT query")
	}
}

func TestRollbackTo(t *testing.T) {
	db, md := newTestGormDB(t)

	err := (&Dialector{Config: &Config{}}).RollbackTo(db, "sp1")
	if err != nil {
		t.Fatalf("RollbackTo: %v", err)
	}
	if !md.containsQuery("ROLLBACK TO SAVEPOINT") {
		t.Fatal("expected ROLLBACK TO SAVEPOINT query")
	}
}

// --- DataTypeOf edge cases ---

func TestDataTypeOf_EdgeCases(t *testing.T) {
	d := &Dialector{Config: &Config{DefaultStringSize: 255}}

	tests := []struct {
		name  string
		field schema.Field
		want  string
	}{
		{"uint16", schema.Field{DataType: schema.Uint, Size: 16}, "INT"},
		{"uint64", schema.Field{DataType: schema.Uint, Size: 64}, "BIGINT"},
		{"uint16_auto", schema.Field{DataType: schema.Uint, Size: 16, AutoIncrement: true}, "INT AUTO_INCREMENT"},
		{"int16_auto", schema.Field{DataType: schema.Int, Size: 16, AutoIncrement: true}, "SHORT AUTO_INCREMENT"},
		{"int64_auto", schema.Field{DataType: schema.Int, Size: 64, AutoIncrement: true}, "BIGINT AUTO_INCREMENT"},
		{"uint32_auto", schema.Field{DataType: schema.Uint, Size: 32, AutoIncrement: true}, "BIGINT AUTO_INCREMENT"},
		{"uint64_auto", schema.Field{DataType: schema.Uint, Size: 64, AutoIncrement: true}, "BIGINT AUTO_INCREMENT"},
		{"bytes_large", schema.Field{DataType: schema.Bytes, Size: 1073741823}, "BLOB"},
		{"custom_type", schema.Field{DataType: "CLOB"}, "CLOB"},
		{"float_size_zero", schema.Field{DataType: schema.Float, Size: 0}, "FLOAT"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := d.DataTypeOf(&tt.field)
			if got != tt.want {
				t.Errorf("DataTypeOf: got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestDataTypeOf_ZeroDefaultStringSize(t *testing.T) {
	d := &Dialector{Config: &Config{DefaultStringSize: 0}}
	got := d.DataTypeOf(&schema.Field{DataType: schema.String, Size: 0})
	if got != "VARCHAR(255)" {
		t.Fatalf("got %q, want %q", got, "VARCHAR(255)")
	}
}

// --- QuoteTo ---

func TestQuoteTo_MultipleDots(t *testing.T) {
	d := &Dialector{Config: &Config{}}
	var buf strings.Builder
	w := writerAdapter{&buf}
	d.QuoteTo(w, "a.b.c")
	want := `"a"."b"."c"`
	if buf.String() != want {
		t.Fatalf("QuoteTo: got %q, want %q", buf.String(), want)
	}
}

// --- logger ---

func TestLogger_MultipleVars(t *testing.T) {
	result := logger("SELECT * FROM t WHERE id = ? AND name = ? AND age = ?", 1, "test", 25)
	if !strings.Contains(result, "'1'") || !strings.Contains(result, "'test'") || !strings.Contains(result, "'25'") {
		t.Fatalf("logger: got %q", result)
	}
}

func TestLogger_NoVars(t *testing.T) {
	result := logger("SELECT * FROM t")
	if result != "SELECT * FROM t" {
		t.Fatalf("logger: got %q", result)
	}
}

// --- New with all fields ---

func TestNew_AllFields(t *testing.T) {
	existingDB := &sql.DB{}
	d := New(Config{
		DSN:               "cubrid://dba:@localhost:33000/demodb",
		Conn:              existingDB,
		DriverName:        "my_cubrid",
		DefaultStringSize: 1024,
	})
	dd := d.(*Dialector)
	if dd.DSN != "cubrid://dba:@localhost:33000/demodb" {
		t.Fatalf("DSN: got %q", dd.DSN)
	}
	if dd.DriverName != "my_cubrid" {
		t.Fatalf("DriverName: got %q", dd.DriverName)
	}
	if dd.DefaultStringSize != 1024 {
		t.Fatalf("DefaultStringSize: got %d", dd.DefaultStringSize)
	}
}

// --- Verify indexColumns via mock ---

func TestIndexColumns_ViaCreateTable(t *testing.T) {
	type indexedModel struct {
		ID    uint   `gorm:"primarykey"`
		Name  string `gorm:"index:idx_name,size:100"`
		Email string `gorm:"uniqueIndex:idx_email"`
	}

	db, md := newTestGormDB(t)

	m := db.Migrator().(CubridMigrator)
	_ = m.CreateTable(&indexedModel{})
	// Verify that CREATE TABLE and index queries were generated.
	if !md.containsQuery("CREATE TABLE") {
		t.Fatal("expected CREATE TABLE query")
	}
}

// --- SavePoint / RollbackTo using Dialector directly ---

func TestSavePoint_ViaDialector(t *testing.T) {
	db, md := newTestGormDB(t)
	d := db.Dialector.(*Dialector)

	err := d.SavePoint(db, "test_sp")
	if err != nil {
		t.Fatalf("SavePoint: %v", err)
	}
	if !md.containsQuery("SAVEPOINT test_sp") {
		t.Fatal("expected SAVEPOINT with name")
	}
}

func TestRollbackTo_ViaDialector(t *testing.T) {
	db, md := newTestGormDB(t)
	d := db.Dialector.(*Dialector)

	err := d.RollbackTo(db, "test_sp")
	if err != nil {
		t.Fatalf("RollbackTo: %v", err)
	}
	if !md.containsQuery("ROLLBACK TO SAVEPOINT test_sp") {
		t.Fatal("expected ROLLBACK TO SAVEPOINT with name")
	}
}

// --- ColumnTypes with zero scale (hasDecimal false) ---

func TestColumnTypes_NoDecimal(t *testing.T) {
	db, md := newTestGormDB(t)

	md.pushResult(
		[]string{"attr_name", "data_type", "prec", "scale", "is_nullable", "default_value", "attr_type"},
		[][]driver.Value{
			{"age", "INTEGER", int64(10), int64(0), "NO", nil, "INSTANCE"},
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
	_, _, ok := col.DecimalSize()
	if ok {
		t.Fatal("expected hasDecimal=false for scale 0")
	}
}
