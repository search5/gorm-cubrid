package cubrid

import (
	"strings"
	"testing"

	"gorm.io/gorm/schema"
)

func TestDialectorName(t *testing.T) {
	d := Open("cubrid://dba:@localhost:33000/demodb")
	if d.Name() != "cubrid" {
		t.Fatalf("Name(): got %q, want %q", d.Name(), "cubrid")
	}
}

func TestOpen(t *testing.T) {
	d := Open("cubrid://dba:@localhost:33000/demodb")
	dd, ok := d.(*Dialector)
	if !ok {
		t.Fatal("expected *Dialector")
	}
	if dd.DSN != "cubrid://dba:@localhost:33000/demodb" {
		t.Fatalf("DSN: got %q", dd.DSN)
	}
}

func TestNew(t *testing.T) {
	d := New(Config{
		DSN:               "cubrid://dba:@localhost:33000/demodb",
		DefaultStringSize: 512,
	})
	dd := d.(*Dialector)
	if dd.DefaultStringSize != 512 {
		t.Fatalf("DefaultStringSize: got %d, want 512", dd.DefaultStringSize)
	}
}

func TestDataTypeOf(t *testing.T) {
	d := &Dialector{Config: &Config{DefaultStringSize: 255}}

	tests := []struct {
		name     string
		field    schema.Field
		want     string
	}{
		{
			"bool",
			schema.Field{DataType: schema.Bool},
			"SHORT",
		},
		{
			"int16",
			schema.Field{DataType: schema.Int, Size: 16},
			"SHORT",
		},
		{
			"int32",
			schema.Field{DataType: schema.Int, Size: 32},
			"INT",
		},
		{
			"int64",
			schema.Field{DataType: schema.Int, Size: 64},
			"BIGINT",
		},
		{
			"int32_auto",
			schema.Field{DataType: schema.Int, Size: 32, AutoIncrement: true},
			"INT AUTO_INCREMENT",
		},
		{
			"uint32",
			schema.Field{DataType: schema.Uint, Size: 32},
			"BIGINT",
		},
		{
			"float32",
			schema.Field{DataType: schema.Float, Size: 32},
			"FLOAT",
		},
		{
			"float64",
			schema.Field{DataType: schema.Float, Size: 64},
			"DOUBLE",
		},
		{
			"numeric",
			schema.Field{DataType: schema.Float, Precision: 10, Scale: 2},
			"NUMERIC(10,2)",
		},
		{
			"string_default",
			schema.Field{DataType: schema.String},
			"VARCHAR(255)",
		},
		{
			"string_100",
			schema.Field{DataType: schema.String, Size: 100},
			"VARCHAR(100)",
		},
		{
			"string_huge",
			schema.Field{DataType: schema.String, Size: 1073741823},
			"STRING",
		},
		{
			"bytes",
			schema.Field{DataType: schema.Bytes, Size: 0},
			"BLOB",
		},
		{
			"bytes_sized",
			schema.Field{DataType: schema.Bytes, Size: 100},
			"VARBIT(800)",
		},
		{
			"time",
			schema.Field{DataType: schema.Time},
			"DATETIME",
		},
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

func TestQuoteTo(t *testing.T) {
	d := &Dialector{Config: &Config{}}
	var buf strings.Builder
	w := writerAdapter{&buf}
	d.QuoteTo(w, "my_table")
	if buf.String() != `"my_table"` {
		t.Fatalf("QuoteTo: got %q, want %q", buf.String(), `"my_table"`)
	}

	buf.Reset()
	d.QuoteTo(w, "schema.my_table")
	if buf.String() != `"schema"."my_table"` {
		t.Fatalf("QuoteTo dotted: got %q, want %q", buf.String(), `"schema"."my_table"`)
	}
}

func TestBindVarTo(t *testing.T) {
	d := &Dialector{Config: &Config{}}
	var buf strings.Builder
	w := writerAdapter{&buf}
	d.BindVarTo(w, nil, 42)
	if buf.String() != "?" {
		t.Fatalf("BindVarTo: got %q, want %q", buf.String(), "?")
	}
}

func TestExplain(t *testing.T) {
	d := &Dialector{Config: &Config{}}
	sql := d.Explain("SELECT * FROM t WHERE id = ? AND name = ?", 1, "Alice")
	if !strings.Contains(sql, "'1'") || !strings.Contains(sql, "'Alice'") {
		t.Fatalf("Explain: got %q", sql)
	}
}

func TestCompileTimeInterfaces(t *testing.T) {
	// These are compile-time checks in cubrid.go, but verify they hold.
	var _ interface{ Name() string } = (*Dialector)(nil)
}

// writerAdapter wraps strings.Builder to satisfy clause.Writer.
type writerAdapter struct {
	b *strings.Builder
}

func (w writerAdapter) WriteByte(c byte) error {
	return w.b.WriteByte(c)
}

func (w writerAdapter) WriteString(s string) (int, error) {
	return w.b.WriteString(s)
}

func (w writerAdapter) Write(p []byte) (int, error) {
	return w.b.Write(p)
}

func (w writerAdapter) AddVar(_ *interface{}, vars ...interface{}) {
}
