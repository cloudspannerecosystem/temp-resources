package samples

// [START spanner_create_record]

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	_ "github.com/googleapis/go-sql-spanner"

	spannergorm "github.com/cloudspannerecosystem/go-spanner-orm/gorm"
	"github.com/cloudspannerecosystem/go-spanner-orm/gorm/samples/models"
)

// CreateRecord created a record in DB
func CreateRecord(ctx context.Context, w io.Writer, dsn string) error {
	// dsn := "projects/my-project/instances/my-instance/databases/my-database"
	db, err := gorm.Open(spannergorm.New(spannergorm.Config{
		DriverName: "spanner",
		DSN:        dsn,
	}), &gorm.Config{PrepareStmt: true})
	if err != nil {
		return err
	}

	// Create a record, with create hook
	singer := models.Singer{Name: "Jinzhu", Age: 18, ActivatedAt: sql.NullTime{Time: time.Now()}}
	result := db.Create(&singer) // pass pointer of data to Create
	fmt.Fprintf(w, "record(s) updated %v.\n", singer.ID)
	fmt.Fprintf(w, "returns error %v.\n", result.Error)
	fmt.Fprintf(w, "returns inserted records count %v.\n", result.RowsAffected)

	// Create Record With Selected Fields, w/o create hook
	// INSERT INTO `singers` (`id`,`name`) VALUES (1, "test")
	singer.ID++
	db.Select("ID", "Name").Create(&singer)

	// Create a record and ignore the values for fields passed to omit, with create hook
	singer.ID++
	db.Omit("Name", "Age", "CreatedAt").Create(&singer)

	// batch insert, with create hook
	var singers = []models.Singer{{ID: 3, Name: "jinzhu1"}, {ID: 4, Name: "jinzhu2"}, {ID: 5, Name: "jinzhu3"}}
	db.CreateInBatches(&singers, 2)

	// create from map, w/o create hook
	db.Model(&models.Singer{}).Create(map[string]interface{}{
		"ID": 6, "Name": "jinzhu", "Age": 18,
	})

	// batch insert from `[]map[string]interface{}{}`, w/o create hook
	db.Model(&models.Singer{}).Create([]map[string]interface{}{
		{"ID": 7, "Name": "jinzhu_1", "Age": 18},
		{"ID": 8, "Name": "jinzhu_2", "Age": 20},
	})

	// Create With Associations
	// INSERT INTO `singers` ...
	// INSERT INTO `songs` ...
	db.Create(&models.Singer{
		ID:   9,
		Name: "jinzhu",
		Song: models.Song{ID: 1, Title: "test"},
	})

	// Do nothing on conflict
	db.Clauses(clause.OnConflict{DoNothing: true}).Create(&models.Singer{
		ID:   9,
		Name: "jinzhu",
		Song: models.Song{ID: 1, Title: "test"},
	})

	// Update columns to default value on `id` conflict
	db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "id"}},
		DoUpdates: clause.Assignments(map[string]interface{}{"Age": 18}),
	}).Create(&models.Singer{
		ID: 9,
	})

	// Update columns to new value on `id` conflict
	db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "id"}},
		DoUpdates: clause.AssignmentColumns([]string{"name", "age"}),
	}).Create(&models.Singer{
		ID: 9,
		MemberNumber: sql.NullString{
			String: "on_id_conflict",
			Valid:  true,
		},
	})

	// Update all columns, except primary keys, to new value on conflict
	db.Clauses(clause.OnConflict{
		UpdateAll: true,
	}).Create(&models.Singer{
		ID:   9,
		Name: "Jinzhu_on_conflict_update_all",
		MemberNumber: sql.NullString{
			String: "on conflict update_all",
			Valid:  true,
		},
		Age:         18,
		ActivatedAt: sql.NullTime{Time: time.Now(), Valid: true},
		Birthday:    sql.NullTime{Time: time.Now(), Valid: true},
	})
	return nil
}

// [END spanner_create_record]
