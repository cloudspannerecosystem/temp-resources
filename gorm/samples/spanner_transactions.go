package samples

// [START spanner_transaction]

import (
	"context"
	"database/sql"
	"io"
	"time"

	"gorm.io/gorm"

	_ "github.com/googleapis/go-sql-spanner"

	spannergorm "github.com/cloudspannerecosystem/go-spanner-orm/gorm"
	"github.com/cloudspannerecosystem/go-spanner-orm/gorm/samples/models"
)

func Transactions(ctx context.Context, w io.Writer, dsn string) error {
	// dsn := "projects/my-project/instances/my-instance/databases/my-database"
	db, err := gorm.Open(spannergorm.New(spannergorm.Config{
		DriverName: "spanner",
		DSN:        dsn,
	}), &gorm.Config{PrepareStmt: true})
	if err != nil {
		return err
	}

	// Continuous session mode
	var singer models.Singer
	tx := db.Session(&gorm.Session{SkipDefaultTransaction: true})
	tx.First(&singer, 1)
	var singers []models.Singer
	tx.Find(&singers)
	tx.Model(&singer).Update("Age", 16)

	singer1 := &models.Singer{
		ID:        12,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
		DeletedAt: sql.NullTime{Valid: true, Time: time.Now()},
		Name:      "save point",
		Age:       22,
		Birthday: sql.NullTime{
			Valid: true,
			Time:  time.Now(),
		},
		MemberNumber: sql.NullString{
			String: "save point",
			Valid:  true,
		},
		ActivatedAt: sql.NullTime{
			Time:  time.Now(),
			Valid: true,
		},
	}
	savePointTx := db.Begin()
	savePointTx.Create(&singer1)
	singer2 := &models.Singer{
		ID:        13,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
		DeletedAt: sql.NullTime{Valid: true, Time: time.Now()},
		Name:      "save point",
		Age:       22,
		Birthday: sql.NullTime{
			Valid: true,
			Time:  time.Now(),
		},
		MemberNumber: sql.NullString{
			String: "save point",
			Valid:  true,
		},
		ActivatedAt: sql.NullTime{
			Time:  time.Now(),
			Valid: true,
		},
	}
	savePointTx.SavePoint("sp1")
	savePointTx.Create(&singer2)
	savePointTx.RollbackTo("sp1") // Rollback user2
	savePointTx.Commit()          // Commit user1
	return nil
}

// [END spanner_transaction]
