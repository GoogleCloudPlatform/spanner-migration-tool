module github.com/cloudspannerecosystem/harbourbridge

go 1.13

require (
	cloud.google.com/go v0.93.3
	cloud.google.com/go/spanner v1.10.0
	github.com/DATA-DOG/go-sqlmock v1.4.1
	github.com/aws/aws-sdk-go v1.34.5
	github.com/go-sql-driver/mysql v1.5.0
	github.com/google/go-cmp v0.5.6
	github.com/google/subcommands v1.2.0
	github.com/gorilla/handlers v1.5.0
	github.com/gorilla/mux v1.7.3
	github.com/lib/pq v1.9.0
	github.com/pganalyze/pg_query_go/v2 v2.0.5
	//github.com/pingcap/parser v3.0.12+incompatible
	github.com/pingcap/parser v0.0.0-20200422082501-7329d80eaf2c
	github.com/pingcap/tidb v1.1.0-beta.0.20200423105559-af376db3dc46
	github.com/sirupsen/logrus v1.5.0 // indirect
	github.com/stretchr/testify v1.6.1
	golang.org/x/crypto v0.0.0-20200622213623-75b288015ac9
	google.golang.org/api v0.54.0
	google.golang.org/genproto v0.0.0-20210827211047-25e5f791fe06
)

// cloud.google.com/go will upgrade grpc to v1.40.0
// We need keep the replacement since google.golang.org/grpc/naming isn't
// available in higher versions.
replace google.golang.org/grpc => google.golang.org/grpc v1.29.1
