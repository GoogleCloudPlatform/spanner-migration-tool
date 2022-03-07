module github.com/cloudspannerecosystem/harbourbridge

go 1.13

require (
	cloud.google.com/go v0.100.2
	cloud.google.com/go/compute v1.5.0 // indirect
	cloud.google.com/go/iam v0.3.0 // indirect
	cloud.google.com/go/spanner v1.30.0
	cloud.google.com/go/storage v1.21.0
	github.com/DATA-DOG/go-sqlmock v1.4.1
	github.com/aws/aws-sdk-go v1.34.5
	github.com/basgys/goxml2json v1.1.0
	github.com/bitly/go-simplejson v0.5.0 // indirect
	github.com/bmizerany/assert v0.0.0-20160611221934-b7ed37b82869 // indirect
	github.com/cespare/xxhash/v2 v2.1.2 // indirect
	github.com/cncf/udpa/go v0.0.0-20220112060539-c52dc94e7fbe // indirect
	github.com/cncf/xds/go v0.0.0-20220121163655-4a2b9fdd466b // indirect
	github.com/denisenkom/go-mssqldb v0.11.0
	github.com/envoyproxy/go-control-plane v0.10.1 // indirect
	github.com/envoyproxy/protoc-gen-validate v0.6.7 // indirect
	github.com/form3tech-oss/jwt-go v3.2.5+incompatible // indirect
	github.com/go-sql-driver/mysql v1.5.0
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/google/go-cmp v0.5.7
	github.com/google/subcommands v1.2.0
	github.com/gorilla/handlers v1.5.0
	github.com/gorilla/mux v1.7.3
	github.com/gorilla/websocket v1.4.1 // indirect
	github.com/lib/pq v1.9.0
	github.com/pganalyze/pg_query_go/v2 v2.0.5
	//github.com/pingcap/parser v3.0.12+incompatible
	github.com/pingcap/parser v0.0.0-20200422082501-7329d80eaf2c
	github.com/pingcap/tidb v1.1.0-beta.0.20200423105559-af376db3dc46
	github.com/sijms/go-ora/v2 v2.2.17
	github.com/sirupsen/logrus v1.5.0 // indirect
	github.com/stretchr/testify v1.7.0
	golang.org/x/crypto v0.0.0-20200622213623-75b288015ac9
	golang.org/x/net v0.0.0-20220225172249-27dd8689420f
	golang.org/x/oauth2 v0.0.0-20220223155221-ee480838109b // indirect
	golang.org/x/sys v0.0.0-20220227234510-4e6760a101f9 // indirect
	golang.org/x/tools v0.1.7 // indirect
	google.golang.org/api v0.70.0
	google.golang.org/genproto v0.0.0-20220304144024-325a89244dc8
	google.golang.org/grpc/examples v0.0.0-20220303195317-63af97474cac // indirect
	google.golang.org/grpc/naming v0.0.0-00010101000000-000000000000 // indirect
	honnef.co/go/tools v0.2.1 // indirect
)

// cloud.google.com/go will upgrade grpc to v1.44.0
// We need keep the replacement since google.golang.org/grpc/naming isn't
// available in v1.29+ versions.
// HACK: Add `naming` package (removed on grpc-go v1.30) to support packages using it (notably pingcap/tidb)
replace google.golang.org/grpc/naming => ./naming

// jwt-go, an indirect dependency imported through pingcap/tidb, is an unmaintained repo which has security issues.
// Replacing it with the fork that tidb's latest master uses and has the security patch.
replace github.com/dgrijalva/jwt-go => github.com/form3tech-oss/jwt-go v3.2.6-0.20210809144907-32ab6a8243d7+incompatible
