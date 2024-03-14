module github.com/GoogleCloudPlatform/spanner-migration-tool

go 1.19

require (
	cloud.google.com/go v0.110.10
	cloud.google.com/go/dataflow v0.9.4
	cloud.google.com/go/datastream v1.10.3
	cloud.google.com/go/monitoring v1.16.3
	cloud.google.com/go/pubsub v1.33.0
	cloud.google.com/go/resourcemanager v1.9.4
	cloud.google.com/go/spanner v1.53.0
	cloud.google.com/go/storage v1.30.1
	github.com/DATA-DOG/go-sqlmock v1.5.2
	github.com/aws/aws-sdk-go v1.44.259
	github.com/basgys/goxml2json v1.1.0
	github.com/cenkalti/backoff/v4 v4.2.1
	github.com/denisenkom/go-mssqldb v0.11.0
	github.com/go-sql-driver/mysql v1.7.1
	github.com/google/go-cmp v0.6.0
	github.com/google/subcommands v1.2.0
	github.com/google/uuid v1.4.0
	github.com/gorilla/handlers v1.5.1
	github.com/gorilla/mux v1.8.0
	github.com/lib/pq v1.9.0
	github.com/pganalyze/pg_query_go/v2 v2.2.0
	github.com/pingcap/tidb v1.1.0-beta.0.20230918090611-71bcc44f77a3
	github.com/pingcap/tidb/parser v0.0.0-20230918090611-71bcc44f77a3
	github.com/pkg/browser v0.0.0-20210911075715-681adbf594b8
	github.com/sijms/go-ora/v2 v2.2.17
	github.com/stretchr/testify v1.8.4
	go.uber.org/ratelimit v0.3.0
	go.uber.org/zap v1.23.0
	golang.org/x/crypto v0.17.0
	golang.org/x/exp v0.0.0-20221023144134-a1e5550cf13e
	golang.org/x/net v0.19.0
	google.golang.org/api v0.153.0
	google.golang.org/genproto v0.0.0-20231120223509-83a465c0220f
	google.golang.org/grpc v1.60.0
	google.golang.org/protobuf v1.31.0
)

require (
	github.com/golang/mock v1.6.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20221227161230-091c0ba34f0a // indirect
	github.com/jackc/puddle/v2 v2.2.1 // indirect
	github.com/stretchr/objx v0.5.0 // indirect
	golang.org/x/mod v0.11.0 // indirect
	golang.org/x/time v0.5.0 // indirect
	golang.org/x/tools v0.10.0 // indirect
)

require (
	cloud.google.com/go/cloudsqlconn v1.5.2
	cloud.google.com/go/compute v1.23.3 // indirect
	cloud.google.com/go/compute/metadata v0.2.3 // indirect
	cloud.google.com/go/iam v1.1.5 // indirect
	cloud.google.com/go/longrunning v0.5.4 // indirect
	github.com/BurntSushi/toml v1.2.1 // indirect
	github.com/benbjohnson/clock v1.3.0 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/bitly/go-simplejson v0.5.0 // indirect
	github.com/census-instrumentation/opencensus-proto v0.4.1 // indirect
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/cncf/udpa/go v0.0.0-20220112060539-c52dc94e7fbe // indirect
	github.com/cncf/xds/go v0.0.0-20230607035331-e9ce68804cb4 // indirect
	github.com/cockroachdb/errors v1.8.1 // indirect
	github.com/cockroachdb/logtags v0.0.0-20190617123548-eb05cc24525f // indirect
	github.com/cockroachdb/redact v1.0.8 // indirect
	github.com/cockroachdb/sentry-go v0.6.1-cockroachdb.2 // indirect
	github.com/cznic/mathutil v0.0.0-20181122101859-297441e03548 // indirect
	github.com/danjacques/gofslock v0.0.0-20191023191349-0a45f885bc37 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/envoyproxy/go-control-plane v0.11.1 // indirect
	github.com/envoyproxy/protoc-gen-validate v1.0.2 // indirect
	github.com/felixge/httpsnoop v1.0.1 // indirect
	github.com/go-ole/go-ole v1.2.6 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang-sql/civil v0.0.0-20220223132316-b832511892a9 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/golang/protobuf v1.5.3 // indirect
	github.com/google/s2a-go v0.1.7 // indirect
	github.com/googleapis/enterprise-certificate-proxy v0.3.2 // indirect
	github.com/googleapis/gax-go/v2 v2.12.0
	github.com/grpc-ecosystem/go-grpc-middleware v1.3.0 // indirect
	github.com/influxdata/tdigest v0.0.1 // indirect
	github.com/jackc/pgx/v5 v5.5.4
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/kr/pretty v0.3.0 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/lufia/plan9stats v0.0.0-20211012122336-39d0f177ccd0 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.1 // indirect
	github.com/opentracing/basictracer-go v1.0.0 // indirect
	github.com/opentracing/opentracing-go v1.2.0 // indirect
	github.com/pingcap/errors v0.11.5-0.20220729040631-518f63d66278 // indirect
	github.com/pingcap/failpoint v0.0.0-20220423142525-ae43b7f4e5c3 // indirect
	github.com/pingcap/kvproto v0.0.0-20230726063044-73d6d7f3756b // indirect
	github.com/pingcap/log v1.1.1-0.20221116035753-734d527bc87c // indirect
	github.com/pingcap/tipb v0.0.0-20221123081521-2fb828910813 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/power-devops/perfstat v0.0.0-20210106213030-5aafc221ea8c // indirect
	github.com/prometheus/client_golang v1.13.0 // indirect
	github.com/prometheus/client_model v0.4.0 // indirect
	github.com/prometheus/common v0.37.0 // indirect
	github.com/prometheus/procfs v0.8.0 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20200410134404-eec4a21b6bb0 // indirect
	github.com/rogpeppe/go-internal v1.6.1 // indirect
	github.com/shirou/gopsutil/v3 v3.22.9 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	github.com/tikv/client-go/v2 v2.0.4-0.20230912041415-9c163cc8574b // indirect
	github.com/tikv/pd/client v0.0.0-20230904040343-947701a32c05 // indirect
	github.com/uber/jaeger-client-go v2.22.1+incompatible // indirect
	github.com/uber/jaeger-lib v2.4.1+incompatible // indirect
	github.com/yusufpapurcu/wmi v1.2.2 // indirect
	go.opencensus.io v0.24.0 // indirect
	go.uber.org/atomic v1.10.0 // indirect
	go.uber.org/multierr v1.8.0 // indirect
	golang.org/x/oauth2 v0.15.0 // indirect
	golang.org/x/sync v0.5.0 // indirect
	golang.org/x/sys v0.15.0 // indirect
	golang.org/x/term v0.15.0 // indirect
	golang.org/x/text v0.14.0 // indirect
	golang.org/x/xerrors v0.0.0-20220907171357-04be3eba64a2 // indirect
	google.golang.org/appengine v1.6.8 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20231106174013-bbf56f31fb17 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20231211222908-989df2bf70f3 // indirect
	gopkg.in/natefinch/lumberjack.v2 v2.0.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
