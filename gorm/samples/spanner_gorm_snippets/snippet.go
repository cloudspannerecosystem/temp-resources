package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/rahul2393/go-spanner-orm/gorm/samples"
)

type command func(ctx context.Context, w io.Writer, dsn string) error

var (
	commands = map[string]command{
		"migrate_model": samples.MigrateModel,
		"create_record": samples.CreateRecord,
		"query_record":  samples.QueryRecord,
		"associations":  samples.Association,
		"transaction":   samples.Transactions,
		"mutations":     samples.Mutations,

		"mysql_migrate_model": samples.MYSQLMigrateModel,
		"mysql_create_record": samples.MySQLCreateRecord,
	}
)

func run(ctx context.Context, w io.Writer, cmd string, dsn string) error {
	cmdFn := commands[cmd]
	if cmdFn == nil {
		flag.Usage()
		os.Exit(2)
	}
	err := cmdFn(ctx, w, dsn)
	if err != nil {
		fmt.Fprintf(w, "%s failed with %v", cmd, err)
	}
	return err
}

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, `Usage: spanner_gorm_snippets <command> <dsn>
	Command can be one of: migrate_model
Examples:
	spanner_gorm_snippets migrate_model projects/my-project/instances/my-instance/databases/example-db
	spanner_gorm_snippets mysql_migrate_model 'user:password@tcp(127.0.0.1:3306)/my_database?charset=utf8mb4&parseTime=True&loc=Local'
`)
	}

	flag.Parse()
	if len(flag.Args()) < 2 {
		flag.Usage()
		os.Exit(2)
	}

	cmd, db := flag.Arg(0), flag.Arg(1)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()
	if err := run(ctx, os.Stdout, cmd, db); err != nil {
		os.Exit(1)
	}
}
