package samples

// [START spanner_query_record]

import (
	"context"
	"fmt"
	"gorm.io/gorm"
	"io"
	"time"

	_ "github.com/googleapis/go-sql-spanner"

	spannergorm "github.com/rahul2393/go-spanner-orm/gorm"
	"github.com/rahul2393/go-spanner-orm/gorm/samples/models"
)

// QueryRecord queries a record in DB
func QueryRecord(ctx context.Context, w io.Writer, dsn string) error {
	// dsn := "projects/my-project/instances/my-instance/databases/my-database"
	db, err := gorm.Open(spannergorm.New(spannergorm.Config{
		DriverName: "spanner",
		DSN:        dsn,
	}), &gorm.Config{PrepareStmt: true})
	if err != nil {
		return err
	}

	var singer models.Singer
	// Get the first record ordered by primary key
	db.First(&singer)
	fmt.Fprintf(w, "Get the first record ordered by primary key %+v\n", singer)
	// SELECT * FROM users ORDER BY id LIMIT 1;

	// Get one record, no specified order
	db.Take(&singer)
	// SELECT * FROM users LIMIT 1;
	fmt.Fprintf(w, "Get one record, no specified order %+v\n", singer)

	// Get last record, ordered by primary key desc
	db.Last(&singer)
	fmt.Fprintf(w, "Get last record, ordered by primary key desc %+v\n", singer)

	// SELECT * FROM `singers` WHERE `singers`.`id` = @p1 ORDER BY `singers`.`id` LIMIT 1
	result := db.First(&singer)
	fmt.Fprintf(w, "Get last record, ordered by primary key desc %+v\n", result)

	// works because model is specified using `db.Model()`
	results := map[string]interface{}{}
	db.Model(&models.Singer{}).First(&results)
	// SELECT * FROM `users` ORDER BY `users`.`id` LIMIT 1
	fmt.Fprintf(w, "Get first record, ordered by primary key desc %+v\n", results)

	var singerById models.Singer
	db.First(&singerById, 9)
	// SELECT * FROM users WHERE id = 9;
	fmt.Fprintf(w, "Get record with ID=9 %+v\n", singerById)

	singerById = models.Singer{}
	db.First(&singerById, 7)
	// SELECT * FROM users WHERE id = 7;
	fmt.Fprintf(w, "Get record with ID=7 %+v\n", singerById)

	var singersByIDOneTwoThree []models.Singer
	db.Find(&singersByIDOneTwoThree, []int{1, 2, 3})
	// SELECT * FROM users WHERE id IN (1,2,3);
	fmt.Fprintf(w, "Get three record %+v\n", singersByIDOneTwoThree)

	singer = models.Singer{}
	db.First(&singer, "id = ?", 5)
	fmt.Fprintf(w, "Get by ID=5 record %+v\n", singer)

	// When the destination object has a primary value, the primary key will be used to build the condition
	singer = models.Singer{ID: 8}
	db.First(&singer)
	// SELECT * FROM users WHERE id = 8;
	fmt.Fprintf(w, "Get by ID=8 record %+v\n", singer)

	var singerByID7 models.Singer
	db.Model(models.Singer{ID: 7}).First(&singerByID7)
	// SELECT * FROM users WHERE id = 7;
	fmt.Fprintf(w, "Get by ID=7 record %+v\n", singer)

	// Get all records
	singers := []models.Singer{}
	db.Find(&singers)
	fmt.Fprintf(w, "all record %+v\n", singers)
	// SELECT * FROM users;

	// Conditions
	// String Conditions
	// Get first matched record
	singer = models.Singer{}
	db.Where("name = ?", "jinzhu").First(&singer)
	// SELECT * FROM users WHERE name = 'jinzhu' ORDER BY id LIMIT 1;
	fmt.Fprintf(w, "all record with name = %+v\n", singers)

	// Get all matched records
	singer = models.Singer{}
	db.Where("name <> ?", "jinzhu").Find(&singer)
	fmt.Fprintf(w, "all record with name <> %+v\n", singers)
	// SELECT * FROM users WHERE name <> 'jinzhu';

	// IN
	singer = models.Singer{}
	db.Where("name IN ?", []string{"jinzhu", "jinzhu2"}).Find(&singer)
	// SELECT * FROM users WHERE name IN ('jinzhu','jinzhu 2');
	fmt.Fprintf(w, "all record with name IN %+v\n", singers)

	// LIKE
	singers = []models.Singer{}
	db.Where("name LIKE ?", "%jin%").Find(&singers)
	// SELECT * FROM users WHERE name LIKE '%jin%';
	fmt.Fprintf(w, "all record with name LIKE %+v\n", singers)

	// AND
	singers = []models.Singer{}
	db.Where("name = ? AND age >= ?", "jinzhu", "22").Find(&singers)
	// SELECT * FROM users WHERE name = 'jinzhu' AND age >= 22;
	fmt.Fprintf(w, "all record with name AND AGE %+v\n", singers)

	// Time
	singers = []models.Singer{}
	lastWeek, _ := time.Parse(time.RFC3339, "2022-11-17T05:22:28.826312Z")
	db.Where("updated_at > ?", lastWeek).Find(&singers)
	// SELECT * FROM users WHERE updated_at > '2000-01-01 00:00:00';
	fmt.Fprintf(w, "all record with updated_at > %+v\n", singers)

	// BETWEEN
	singers = []models.Singer{}
	db.Where("created_at BETWEEN ? AND ?", lastWeek, time.Now()).Find(&singers)
	// SELECT * FROM users WHERE created_at BETWEEN '
	fmt.Fprintf(w, "all record with created_at BETWEEN %+v\n", singers)

	// Scanning results into a struct works similarly to the way we use Find
	type Result struct {
		Name string
		Age  int
	}

	var r Result
	db.Table("singers").Select("name", "age").Where("name = ?", "jinzhu3").Scan(&r)
	fmt.Fprintf(w, "record with name jinzhu3 %+v\n", r)

	r = Result{}
	// Raw SQL
	if err := db.Raw("SELECT name, age FROM singers WHERE name = ?", "jinzhu_2").Scan(&r).Error; err != nil {
		return err
	}
	fmt.Fprintf(w, "record with name jinzhu_2 %+v\n", r)
	return nil
}

// [END spanner_query_record]
