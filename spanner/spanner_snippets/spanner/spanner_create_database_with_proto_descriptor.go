// Copyright 2023 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package spanner

// [START spanner_create_database_with_proto_descriptor]
import (
	"context"
	"fmt"
	"io"
	"regexp"

	database "cloud.google.com/go/spanner/admin/database/apiv1"
	adminpb "cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"google.golang.org/api/option"
)

func createDatabaseWithProtoDescriptor(ctx context.Context, w io.Writer, r io.Reader, db string) error {
	matches := regexp.MustCompile("^(.*)/databases/(.*)$").FindStringSubmatch(db)
	if matches == nil || len(matches) != 3 {
		return fmt.Errorf("Invalid database id %s", db)
	}

	endpoint := "staging-wrenchworks.sandbox.googleapis.com:443"
	// TODO(harsha): Replace below by NewDatabaseAdminClient once visibility label TRUSTED_TESTER_PROTO is removed before GA
	adminClient, err := database.NewDatabaseAdminRESTClient(ctx, option.WithEndpoint(endpoint))
	if err != nil {
		return err
	}
	defer adminClient.Close()

	/*Reading proto descriptor directly
	descriptor, err := os.ReadFile("/usr/local/google/home/sriharshach/github/Go/golang-samples-proto-support-v2/spanner/spanner_snippets/spanner/testdata/protos/descriptor.pb")
	if err != nil {
		return err
	}*/

	protoFileDescriptor, err := io.ReadAll(r)
	if err != nil {
		return err
	}

	op, err := adminClient.CreateDatabase(ctx, &adminpb.CreateDatabaseRequest{
		Parent:          matches[1],
		CreateStatement: "CREATE DATABASE `" + matches[2] + "`",
		ExtraStatements: []string{
			`CREATE PROTO BUNDLE (
  			spanner.examples.music.SingerInfo,
  			spanner.examples.music.Genre,
			)`,
			`CREATE TABLE Singers (
				SingerId   INT64 NOT NULL,
				FirstName  STRING(1024),
				LastName   STRING(1024),
				SingerInfo spanner.examples.music.SingerInfo,
				SingerGenre spanner.examples.music.Genre,
			) PRIMARY KEY (SingerId)`,
			`CREATE TABLE SingersArray (
				SingerId   INT64 NOT NULL,
				FirstName  STRING(1024),
				LastName   STRING(1024),
				SingerInfo ARRAY<spanner.examples.music.SingerInfo>,
				SingerGenre ARRAY<spanner.examples.music.Genre>,
			) PRIMARY KEY (SingerId)`,
		},
		ProtoDescriptors: protoFileDescriptor,
	})
	if err != nil {
		return err
	}
	if _, err := op.Wait(ctx); err != nil {
		return err
	}
	fmt.Fprintf(w, "Created database with proto descriptors[%s]\n", db)
	return nil
}

// [END spanner_create_database_with_proto_descriptor]
