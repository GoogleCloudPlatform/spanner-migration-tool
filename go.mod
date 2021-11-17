module github.com/cloudspannerecosystem/harbourbridge

go 1.13

require (
	cloud.google.com/go v0.94.1
	cloud.google.com/go/spanner v1.10.0
	cloud.google.com/go/storage v1.17.0
	github.com/DATA-DOG/go-sqlmock v1.4.1
	github.com/aws/aws-sdk-go v1.34.5
	github.com/form3tech-oss/jwt-go v3.2.5+incompatible // indirect
	github.com/go-sql-driver/mysql v1.5.0
	github.com/google/go-cmp v0.5.6
	github.com/google/subcommands v1.2.0
	github.com/gorilla/handlers v1.5.0
	github.com/gorilla/mux v1.7.3
	github.com/gorilla/websocket v1.4.1 // indirect
	github.com/lib/pq v1.9.0
	github.com/pganalyze/pg_query_go/v2 v2.0.5
	//github.com/pingcap/parser v3.0.12+incompatible
	github.com/pingcap/parser v0.0.0-20200422082501-7329d80eaf2c
	github.com/pingcap/tidb v1.1.0-beta.0.20200423105559-af376db3dc46
	github.com/sirupsen/logrus v1.5.0 // indirect
	github.com/stretchr/testify v1.6.1
	golang.org/x/crypto v0.0.0-20200622213623-75b288015ac9
	golang.org/x/tools v0.1.7 // indirect
	google.golang.org/api v0.57.0
	google.golang.org/genproto v0.0.0-20210921142501-181ce0d877f6
)

// cloud.google.com/go will upgrade grpc to v1.40.0
// We need keep the replacement since google.golang.org/grpc/naming isn't
// available in higher versions.
replace google.golang.org/grpc => google.golang.org/grpc v1.29.1

// Temporary workaround until PG support is available in
// cloud.google.com/go/spanner public release.
// Versions below corresponds to 'type-annotation' branch in the forked repos.
replace cloud.google.com/go => github.com/hengfengli/google-cloud-go v0.48.1-0.20210903130221-989a19403c8c

replace cloud.google.com/go/spanner => github.com/hengfengli/google-cloud-go/spanner v1.1.1-0.20210903130221-989a19403c8c

replace google.golang.org/genproto => github.com/hengfengli/go-genproto v0.0.0-20210914051132-d2ab1d87ef40

// jwt-go, an indirect dependency imported through pingcap/tidb, is an unmaintained repo which has security issues.
// Replacing it with the fork that tidb's latest master uses and has the security patch.
replace github.com/dgrijalva/jwt-go => github.com/form3tech-oss/jwt-go v3.2.6-0.20210809144907-32ab6a8243d7+incompatible
