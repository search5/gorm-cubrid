package cubrid

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
)

// mockDriver is a minimal SQL driver for testing GORM dialector methods.
// It records executed queries and returns pre-configured row results.
type mockDriver struct {
	mu       sync.Mutex
	queries  []string
	results  []mockResult
	execFn   func(query string) // optional callback when exec/query is called
}

type mockResult struct {
	columns []string
	rows    [][]driver.Value
}

var mockDriverCounter atomic.Int64

// newMockDriverDB registers a fresh mock driver and returns a sql.DB backed by it.
func newMockDriverDB() (*sql.DB, *mockDriver) {
	md := &mockDriver{}
	name := fmt.Sprintf("mock_cubrid_%d", mockDriverCounter.Add(1))
	sql.Register(name, md)
	db, _ := sql.Open(name, "")
	return db, md
}

// pushResult adds a result set that will be returned by the next query.
func (m *mockDriver) pushResult(columns []string, rows [][]driver.Value) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.results = append(m.results, mockResult{columns: columns, rows: rows})
}

// popResult removes and returns the next result.
func (m *mockDriver) popResult() *mockResult {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.results) == 0 {
		return nil
	}
	r := m.results[0]
	m.results = m.results[1:]
	return &r
}

// recordQuery stores the executed query.
func (m *mockDriver) recordQuery(q string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.queries = append(m.queries, q)
	if m.execFn != nil {
		m.execFn(q)
	}
}

// lastQuery returns the last recorded query.
func (m *mockDriver) lastQuery() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.queries) == 0 {
		return ""
	}
	return m.queries[len(m.queries)-1]
}

// containsQuery checks if any recorded query contains the substring.
func (m *mockDriver) containsQuery(sub string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, q := range m.queries {
		if strings.Contains(q, sub) {
			return true
		}
	}
	return false
}

// --- driver.Driver ---

func (m *mockDriver) Open(name string) (driver.Conn, error) {
	return &mockConn{driver: m}, nil
}

// --- driver.Conn ---

type mockConn struct {
	driver *mockDriver
}

func (c *mockConn) Prepare(query string) (driver.Stmt, error) {
	c.driver.recordQuery(query)
	return &mockStmt{conn: c, query: query}, nil
}

func (c *mockConn) Close() error { return nil }
func (c *mockConn) Begin() (driver.Tx, error) {
	return &mockTx{}, nil
}

// QueryContext implements driver.QueryerContext for GORM compatibility.
func (c *mockConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	c.driver.recordQuery(query)
	result := c.driver.popResult()
	if result == nil {
		return &mockRows{columns: []string{}, rows: nil}, nil
	}
	return &mockRows{columns: result.columns, rows: result.rows}, nil
}

// ExecContext implements driver.ExecerContext for GORM compatibility.
func (c *mockConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	c.driver.recordQuery(query)
	return mockExecResult{}, nil
}

// --- driver.Stmt ---

type mockStmt struct {
	conn  *mockConn
	query string
}

func (s *mockStmt) Close() error                               { return nil }
func (s *mockStmt) NumInput() int                               { return -1 }
func (s *mockStmt) Exec(args []driver.Value) (driver.Result, error) {
	s.conn.driver.recordQuery(s.query)
	return mockExecResult{}, nil
}
func (s *mockStmt) Query(args []driver.Value) (driver.Rows, error) {
	s.conn.driver.recordQuery(s.query)
	result := s.conn.driver.popResult()
	if result == nil {
		return &mockRows{columns: []string{}, rows: nil}, nil
	}
	return &mockRows{columns: result.columns, rows: result.rows}, nil
}

// --- driver.Rows ---

type mockRows struct {
	columns []string
	rows    [][]driver.Value
	idx     int
}

func (r *mockRows) Columns() []string { return r.columns }
func (r *mockRows) Close() error      { return nil }
func (r *mockRows) Next(dest []driver.Value) error {
	if r.idx >= len(r.rows) {
		return io.EOF
	}
	row := r.rows[r.idx]
	r.idx++
	copy(dest, row)
	return nil
}

// --- driver.Tx ---

type mockTx struct{}

func (t *mockTx) Commit() error   { return nil }
func (t *mockTx) Rollback() error { return nil }

// --- driver.Result ---

type mockExecResult struct{}

func (r mockExecResult) LastInsertId() (int64, error) { return 0, nil }
func (r mockExecResult) RowsAffected() (int64, error) { return 0, nil }
