// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package conversion

import (
	"database/sql"
	"testing"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/profiles"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewCloudSQLPostgresConnConfig(t *testing.T) {
	t.Run("preserves complex special-character credentials verbatim", func(t *testing.T) {
		pg := profiles.SourceProfileConnectionCloudSQLPostgreSQL{
			User: `ad"min '1\2=3`,
			Db:   `my/db "name,1'2\3`,
			Pwd:  "myPass 's3cret\"Tail,1=2\\end \t\n/db?sslmode=require\\",
		}

		cfg, err := newCloudSQLPostgresConnConfig(pg)
		require.NoError(t, err)
		assert.Equal(t, pg.User, cfg.User)
		assert.Equal(t, pg.Db, cfg.Database)
		assert.Equal(t, pg.Pwd, cfg.Password)

		dbURI := stdlib.RegisterConnConfig(cfg)
		defer stdlib.UnregisterConnConfig(dbURI)
		db, err := sql.Open("pgx", dbURI)
		require.NoError(t, err)
		assert.NoError(t, db.Close())
	})

	t.Run("IAM empty password ignores ambient PGPASSWORD", func(t *testing.T) {
		t.Setenv("PGPASSWORD", "ambient_env_secret")
		pg := profiles.SourceProfileConnectionCloudSQLPostgreSQL{
			User: "iam-user@example.iam",
			Db:   "postgres",
			Pwd:  "",
		}

		cfg, err := newCloudSQLPostgresConnConfig(pg)
		require.NoError(t, err)
		assert.Equal(t, pg.User, cfg.User)
		assert.Equal(t, pg.Db, cfg.Database)
		assert.Empty(t, cfg.Password)
	})
}
