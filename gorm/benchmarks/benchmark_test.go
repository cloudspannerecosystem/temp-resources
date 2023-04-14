package benchmarks

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/google/uuid"
	"math/rand"
	"os"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	_ "github.com/googleapis/go-sql-spanner"
	"google.golang.org/grpc/codes"
	"gorm.io/datatypes"
	"gorm.io/gorm"

	spannergorm "github.com/cloudspannerecosystem/go-spanner-orm/gorm"
	databasepb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
)

var benchmarkProjectId, benchmarkInstanceId, benchmarkDatabaseId string
var allIds []string

// BaseModel is embedded in all other models to add common database fields.
type BaseModel struct {
	ID string `gorm:"primaryKey;autoIncrement:false"`
	// CreatedAt and UpdatedAt are managed automatically by gorm.
	CreatedAt time.Time
	UpdatedAt time.Time
}

type Singer struct {
	BaseModel
	FirstName sql.NullString
	LastName  string
	FullName  string `gorm:"->;type:STRING(100) AS (ARRAY_TO_STRING([first_name, last_name], \" \")) STORED;default:(-);"`
	Active    bool
	Albums    []Album
}

type Album struct {
	BaseModel
	Title        string
	ReleaseDate  datatypes.Date
	CoverPicture []byte
	SingerId     string
	Singer       Singer
}

func TestMain(m *testing.M) {
	fmt.Printf("Initializing benchmarks...\n")
	if err := setup(); err != nil {
		fmt.Printf("failed to initialize benchmark: %v", err)
		os.Exit(1)
	}
	res := m.Run()
	os.Exit(res)
}

func setup() error {
	benchmarkProjectId, benchmarkInstanceId, benchmarkDatabaseId = os.Getenv("BENCHMARK_PROJECT_ID"), os.Getenv("BENCHMARK_INSTANCE_ID"), os.Getenv("BENCHMARK_DATABASE_ID")
	if benchmarkProjectId == "" {
		return fmt.Errorf("missing BENCHMARK_PROJECT_ID")
	}
	if benchmarkInstanceId == "" {
		return fmt.Errorf("missing BENCHMARK_INSTANCE_ID")
	}
	if benchmarkDatabaseId == "" {
		return fmt.Errorf("missing BENCHMARK_DATABASE_ID")
	}
	if err := createDb(benchmarkProjectId, benchmarkInstanceId, benchmarkDatabaseId); err != nil {
		return fmt.Errorf("Failed to create benchmark database: %v\n", err)
	}

	db, err := gorm.Open(spannergorm.New(spannergorm.Config{
		DriverName: "spanner",
		DSN:        fmt.Sprintf("projects/%s/instances/%s/databases/%s", benchmarkProjectId, benchmarkInstanceId, benchmarkDatabaseId),
	}), &gorm.Config{PrepareStmt: true})
	if err != nil {
		return err
	}

	if err = db.AutoMigrate(&Singer{}, &Album{}); err != nil {
		return err
	}
	fmt.Print("Deleting existing albums\n")
	if err := deleteAlbums(db); err != nil {
		return fmt.Errorf("failed to delete albums: %v", err)
	}

	batches := 10
	count := 1000
	total := batches * count
	var c int64
	if err := db.Model(&Singer{}).Count(&c).Error; err != nil {
		return err
	}
	if c == int64(total) {
		return selectAllSingerIds(db, total)
	}

	fmt.Print("Deleting existing singers\n")
	if err := deleteSingers(db); err != nil {
		return fmt.Errorf("Failed to delete existing singers from database: %v\n", err)
	}

	// Insert 10,000 test records.
	fmt.Printf("Inserting %v test records in %v batches\n", total, batches)
	for batch := 0; batch < batches; batch++ {
		ids, err := createRandomSingers(db, count)
		if err != nil {
			return fmt.Errorf("Failed to insert a batch of %v singers: %v\n", count, err)
		}
		allIds = append(allIds, ids...)
		fmt.Printf("Inserted %v singers\n", (batch+1)*count)
	}
	return nil
}

func createDb(projectId, instanceId, databaseId string) error {
	ctx := context.Background()
	databaseAdminClient, err := database.NewDatabaseAdminClient(ctx)
	if err != nil {
		return err
	}
	defer databaseAdminClient.Close()

	if _, err := databaseAdminClient.GetDatabase(ctx, &databasepb.GetDatabaseRequest{
		Name: fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectId, instanceId, databaseId),
	}); spanner.ErrCode(err) != codes.NotFound {
		return err
	}

	fmt.Print("Database does not yet exists\n")
	fmt.Printf("Creating database %s\n", databaseId)
	opDB, err := databaseAdminClient.CreateDatabase(ctx, &databasepb.CreateDatabaseRequest{
		Parent:          fmt.Sprintf("projects/%s/instances/%s", projectId, instanceId),
		CreateStatement: fmt.Sprintf("CREATE DATABASE `%s`", databaseId),
	})
	if err != nil {
		return err
	}
	// Wait for the database creation to finish.
	if _, err := opDB.Wait(ctx); err != nil {
		return fmt.Errorf("waiting for database creation to finish failed: %v", err)
	}

	fmt.Printf("Finished creating database %s\n", databaseId)
	return nil
}

func deleteSingers(db *gorm.DB) error {
	return deleteAll(db, "Singers")
}

func deleteAlbums(db *gorm.DB) error {
	return deleteAll(db, "Albums")
}

func deleteAll(db *gorm.DB, table string) error {
	return db.Exec(fmt.Sprintf("DELETE FROM `%s` WHERE TRUE", table)).Error
}

func selectAllSingerIds(db *gorm.DB, count int) error {
	allIds = make([]string, count)
	return db.Model(&Singer{}).Pluck("ID", &allIds).Error
}

func createRandomSingers(db *gorm.DB, count int) ([]string, error) {
	ids, singers := createRandomSingerMutations(count)
	return ids, db.CreateInBatches(singers, 100).Error
}

func createRandomSingerMutations(count int) ([]string, []*Singer) {
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	firstNames := []string{"Pete", "Alice", "John", "Ethel", "Trudy", "Naomi", "Wendy", "Ruben", "Thomas", "Elly", "Cora", "Elise", "April", "Libby", "Alexandra", "Shania"}
	lastNames := []string{"Wendelson", "Allison", "Peterson", "Johnson", "Henderson", "Ericsson", "Aronson", "Tennet", "Courtou", "Mcdonald", "Berry", "Ramirez"}

	singers := make([]*Singer, count)
	ids := make([]string, count)
	for i := 0; i < count; i++ {
		ids[i] = uuid.NewString()
		singers[i] = &Singer{
			BaseModel: BaseModel{ID: ids[i]},
			FirstName: sql.NullString{String: firstNames[rnd.Intn(len(firstNames))], Valid: true},
			LastName:  lastNames[rnd.Intn(len(lastNames))],
		}
	}
	return ids, singers
}
