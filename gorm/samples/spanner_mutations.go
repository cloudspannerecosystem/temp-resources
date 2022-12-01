package samples

// [START spanner_mutations]

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"time"

	"gorm.io/gorm"

	"cloud.google.com/go/spanner"
	spannerdriver "github.com/googleapis/go-sql-spanner"

	spannergorm "github.com/rahul2393/go-spanner-orm/gorm"
)

func Mutations(ctx context.Context, w io.Writer, dsn string) error {
	// dsn := "projects/my-project/instances/my-instance/databases/my-database"
	db, err := gorm.Open(spannergorm.New(spannergorm.Config{
		DriverName: "spanner",
		DSN:        dsn,
	}), &gorm.Config{PrepareStmt: true})
	if err != nil {
		return err
	}
	d, _ := db.DB()
	// Get a connection so that we can get access to the Spanner specific connection interface SpannerConn.
	conn, err := d.Conn(ctx)
	if err != nil {
		return err
	}
	// Mutations can be written outside an explicit transaction using SpannerConn#Apply.
	var commitTimestamp time.Time
	if err := conn.Raw(func(driverConn interface{}) error {
		spannerConn, ok := driverConn.(spannerdriver.SpannerConn)
		if !ok {
			return fmt.Errorf("unexpected driver connection %v, expected SpannerConn", driverConn)
		}
		commitTimestamp, err = spannerConn.Apply(ctx, []*spanner.Mutation{
			spanner.Insert("Singers", []string{"Id", "Name"}, []interface{}{int64(10), "Richard Moore"}),
			spanner.Insert("Singers", []string{"Id", "Name"}, []interface{}{int64(11), "Alice Henderson"}),
		})
		return err
	}); err != nil {
		return err
	}
	fmt.Printf("The transaction with two singer mutations was committed at %v\n", commitTimestamp)

	// Mutations can also be executed as part of a read/write transaction.
	// Note: The transaction is started using the connection that we had obtained. This is necessary in order to
	// ensure that the conn.Raw call below will use the same connection as the one that just started the transaction.
	tx, err := conn.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return err
	}

	// Get the name of a singer and update it using a mutation.
	id := int64(10)
	row := tx.QueryRowContext(ctx, "SELECT Name FROM Singers WHERE Id=@id", id)
	var name string
	if err := row.Scan(&name); err != nil {
		return err
	}
	if err := conn.Raw(func(driverConn interface{}) error {
		spannerConn, ok := driverConn.(spannerdriver.SpannerConn)
		if !ok {
			return fmt.Errorf("unexpected driver connection %v, expected SpannerConn", driverConn)
		}
		return spannerConn.BufferWrite([]*spanner.Mutation{
			spanner.Update("Singers", []string{"Id", "Name"}, []interface{}{id, name + "-Henderson"}),
		})
	}); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	fmt.Print("Updated the name of the first singer\n")

	// Read back the updated row.
	// Scanning results into a struct works similarly to the way we use Find
	type Result struct {
		SingerId int64
		Name     string
	}

	var r Result
	if err := db.Raw("SELECT SingerId, Name FROM Singers WHERE SingerId = ?", id).Scan(&r).Error; err != nil {
		return err
	}
	fmt.Printf("Updated singer: %v %v\n", id, name)
	return nil
}

// [END spanner_mutations]
