package samples

// [START spanner_migrate_model]

import (
	"context"
	"io"

	"gorm.io/gorm"

	_ "github.com/googleapis/go-sql-spanner"

	spannergorm "github.com/cloudspannerecosystem/go-spanner-orm/gorm"
	"github.com/cloudspannerecosystem/go-spanner-orm/gorm/samples/models"
)

// CREATE TABLE singers (
//
//	id INT64,
//	created_at TIMESTAMP,
//	updated_at TIMESTAMP,
//	deleted_at TIMESTAMP,
//	name STRING(MAX),
//	email STRING(MAX),
//	age INT64,
//	birthday TIMESTAMP,
//	member_number STRING(MAX),
//	activated_at TIMESTAMP,
//
// ) PRIMARY KEY(id);
//
// CREATE INDEX idx_singers_deleted_at ON singers(deleted_at);

// MigrateModel validates the GORM struct declaration with Spanner and run the migrations
func MigrateModel(ctx context.Context, w io.Writer, dsn string) error {
	// dsn := "projects/my-project/instances/my-instance/databases/my-database"
	db, err := gorm.Open(spannergorm.New(spannergorm.Config{
		DriverName: "spanner",
		DSN:        dsn,
	}), &gorm.Config{PrepareStmt: true})
	if err != nil {
		return err
	}
	// Automatically create the "songs" table based on the `Account`
	// model.
	return db.AutoMigrate(&models.Singer{}, &models.Song{})
}

// [END spanner_migrate_model]
