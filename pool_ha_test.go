//go:build integration

package cubrid

import (
	"testing"

	cubriddriver "github.com/search5/cubrid-go"
	"gorm.io/gorm"
)

const integrationDSN = "cubrid://dba:@localhost:33000/cubdb"

func TestIntegrationGormPool(t *testing.T) {
	db, err := gorm.Open(OpenPool(cubriddriver.PoolConfig{
		DSN:     integrationDSN,
		MaxOpen: 5,
		MaxIdle: 2,
	}), &gorm.Config{})
	if err != nil {
		t.Fatalf("gorm.Open with Pool: %v", err)
	}

	sqlDB, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	defer sqlDB.Close()

	// Simple query via GORM.
	var result int
	tx := db.Raw("SELECT 1 + 1").Scan(&result)
	if tx.Error != nil {
		t.Fatalf("query: %v", tx.Error)
	}
	if result != 2 {
		t.Fatalf("expected 2, got %d", result)
	}
	t.Log("GORM + Pool: query succeeded")
}

func TestIntegrationGormPoolAutoMigrate(t *testing.T) {
	db, err := gorm.Open(OpenPool(cubriddriver.PoolConfig{
		DSN:     integrationDSN,
		MaxOpen: 5,
		MaxIdle: 2,
	}), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}

	sqlDB, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	defer sqlDB.Close()

	type PoolTestModel struct {
		ID   int    `gorm:"primaryKey;autoIncrement"`
		Name string `gorm:"type:VARCHAR(100)"`
	}

	// Drop if exists.
	db.Exec("DROP TABLE IF EXISTS \"pool_test_models\"")

	if err := db.AutoMigrate(&PoolTestModel{}); err != nil {
		t.Fatalf("AutoMigrate: %v", err)
	}

	// Insert and read back.
	db.Create(&PoolTestModel{Name: "pool_test"})

	var m PoolTestModel
	if err := db.First(&m).Error; err != nil {
		t.Fatal(err)
	}
	if m.Name != "pool_test" {
		t.Fatalf("expected pool_test, got %q", m.Name)
	}

	// Cleanup.
	db.Exec("DROP TABLE IF EXISTS \"pool_test_models\"")
	t.Log("GORM + Pool: AutoMigrate + CRUD succeeded")
}

func TestIntegrationGormHA(t *testing.T) {
	db, err := gorm.Open(OpenHA(cubriddriver.HAConfig{
		DSN:              integrationDSN + "?ha=true",
		MaxOpenPerBroker: 3,
	}, false), &gorm.Config{})
	if err != nil {
		t.Fatalf("gorm.Open with HA: %v", err)
	}

	sqlDB, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	defer sqlDB.Close()

	var result int
	tx := db.Raw("SELECT 42").Scan(&result)
	if tx.Error != nil {
		t.Fatalf("query: %v", tx.Error)
	}
	if result != 42 {
		t.Fatalf("expected 42, got %d", result)
	}
	t.Log("GORM + HA: query succeeded")
}
