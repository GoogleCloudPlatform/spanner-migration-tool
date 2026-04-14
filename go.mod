module github.com/GoogleCloudPlatform/spanner-migration-tool

go 1.25.8

require (
	cloud.google.com/go v0.123.0
	cloud.google.com/go/aiplatform v1.120.0
	cloud.google.com/go/dataflow v0.14.0
	cloud.google.com/go/datastream v1.15.1
	cloud.google.com/go/monitoring v1.24.3
	cloud.google.com/go/pubsub v1.50.1
	cloud.google.com/go/resourcemanager v1.10.7
	cloud.google.com/go/secretmanager v1.16.0
	cloud.google.com/go/spanner v1.88.0
	cloud.google.com/go/storage v1.56.0
	cloud.google.com/go/vertexai v0.13.3
	github.com/DATA-DOG/go-sqlmock v1.5.2
	github.com/aws/aws-sdk-go v1.55.5
	github.com/basgys/goxml2json v1.1.0
	github.com/cenkalti/backoff/v4 v4.2.1
	github.com/denisenkom/go-mssqldb v0.11.0
	github.com/dominikbraun/graph v0.23.0
	github.com/go-sql-driver/mysql v1.8.1
	github.com/gocql/gocql v1.7.0
	github.com/google/go-cmp v0.7.0
	github.com/google/subcommands v1.2.0
	github.com/google/uuid v1.6.0
	github.com/googleapis/go-spanner-cassandra v0.1.0
	github.com/gorilla/handlers v1.5.1
	github.com/gorilla/mux v1.8.1
	github.com/lib/pq v1.9.0
	github.com/pganalyze/pg_query_go/v6 v6.1.0
	github.com/pingcap/tidb v1.1.0-beta.0.20251126154744-e4e814fdc0af
	github.com/pingcap/tidb/pkg/parser v0.0.0-20251126154744-e4e814fdc0af
	github.com/pkg/browser v0.0.0-20240102092130-5ac0b6a4141c
	github.com/sijms/go-ora/v2 v2.2.17
	github.com/smacker/go-tree-sitter v0.0.0-20240827094217-dd81d9e9be82
	github.com/stretchr/testify v1.11.1
	go.uber.org/ratelimit v0.3.1
	go.uber.org/zap v1.27.0
	golang.org/x/crypto v0.49.0
	golang.org/x/exp v0.0.0-20240909161429-701f63a606c0
	golang.org/x/net v0.52.0
	golang.org/x/tools v0.42.0
	google.golang.org/api v0.274.0
	google.golang.org/genproto v0.0.0-20260319201613-d00831a3d3e7
	google.golang.org/grpc v1.80.0
	google.golang.org/protobuf v1.36.11
)

require (
	cel.dev/expr v0.25.1 // indirect
	cloud.google.com/go/auth v0.18.2 // indirect
	cloud.google.com/go/auth/oauth2adapt v0.2.8 // indirect
	cloud.google.com/go/pubsub/v2 v2.0.0 // indirect
	filippo.io/edwards25519 v1.1.1 // indirect
	github.com/GoogleCloudPlatform/grpc-gcp-go/grpcgcp v1.6.0 // indirect
	github.com/GoogleCloudPlatform/opentelemetry-operations-go/detectors/gcp v1.31.0 // indirect
	github.com/GoogleCloudPlatform/opentelemetry-operations-go/exporter/metric v0.53.0 // indirect
	github.com/GoogleCloudPlatform/opentelemetry-operations-go/internal/resourcemapping v0.53.0 // indirect
	github.com/VividCortex/ewma v1.2.0 // indirect
	github.com/cloudfoundry/gosigar v1.3.6 // indirect
	github.com/coreos/go-semver v0.3.1 // indirect
	github.com/coreos/go-systemd/v22 v22.5.0 // indirect
	github.com/datastax/go-cassandra-native-protocol v0.0.0-20240903140133-605a850e203b // indirect
	github.com/dgryski/go-farm v0.0.0-20240924180020-3414d57e47da // indirect
	github.com/docker/go-units v0.5.0 // indirect
	github.com/envoyproxy/go-control-plane/envoy v1.36.0 // indirect
	github.com/go-jose/go-jose/v4 v4.1.4 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/golang/snappy v1.0.0 // indirect
	github.com/google/btree v1.1.2 // indirect
	github.com/google/martian/v3 v3.3.3 // indirect
	github.com/hailocab/go-hostpool v0.0.0-20160125115350-e80d13ce29ed // indirect
	github.com/hashicorp/golang-lru v1.0.2 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/pingcap/sysutil v1.0.1-0.20240311050922-ae81ee01f3a5 // indirect
	github.com/planetscale/vtprotobuf v0.6.1-0.20240319094008-0393e58bdf10 // indirect
	github.com/shoenig/go-m1cpu v0.2.1 // indirect
	github.com/spiffe/go-spiffe/v2 v2.6.0 // indirect
	github.com/stretchr/objx v0.5.2 // indirect
	github.com/tiancaiamao/gp v0.0.0-20221230034425-4025bc8a4d4a // indirect
	github.com/tklauser/go-sysconf v0.3.12 // indirect
	github.com/tklauser/numcpus v0.6.1 // indirect
	github.com/twmb/murmur3 v1.1.6 // indirect
	go.etcd.io/etcd/api/v3 v3.5.12 // indirect
	go.etcd.io/etcd/client/pkg/v3 v3.5.12 // indirect
	go.etcd.io/etcd/client/v3 v3.5.12 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/contrib/detectors/gcp v1.39.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.61.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.61.0 // indirect
	go.opentelemetry.io/otel v1.42.0 // indirect
	go.opentelemetry.io/otel/metric v1.42.0 // indirect
	go.opentelemetry.io/otel/sdk v1.42.0 // indirect
	go.opentelemetry.io/otel/sdk/metric v1.42.0 // indirect
	go.opentelemetry.io/otel/trace v1.42.0 // indirect
	golang.org/x/mod v0.33.0 // indirect
	golang.org/x/time v0.15.0 // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
	modernc.org/mathutil v1.7.1 // indirect
)

require (
	cloud.google.com/go/cloudsqlconn v1.14.0
	cloud.google.com/go/compute/metadata v0.9.0 // indirect
	cloud.google.com/go/iam v1.5.3 // indirect
	cloud.google.com/go/longrunning v0.8.0 // indirect
	github.com/benbjohnson/clock v1.3.5 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/bitly/go-simplejson v0.5.0 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/cncf/xds/go v0.0.0-20251210132809-ee656c7534f5 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/envoyproxy/protoc-gen-validate v1.3.0 // indirect
	github.com/felixge/httpsnoop v1.0.4 // indirect
	github.com/go-ole/go-ole v1.3.0 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang-sql/civil v0.0.0-20220223132316-b832511892a9 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/google/s2a-go v0.1.9 // indirect
	github.com/googleapis/enterprise-certificate-proxy v0.3.14 // indirect
	github.com/googleapis/gax-go/v2 v2.21.0
	github.com/grpc-ecosystem/go-grpc-middleware v1.4.0 // indirect
	github.com/jackc/pgx/v5 v5.7.2
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/lufia/plan9stats v0.0.0-20230326075908-cb1d2100619a // indirect
	github.com/opentracing/opentracing-go v1.2.0 // indirect
	github.com/pingcap/errors v0.11.5-0.20241219054535-6b8c588c3122 // indirect
	github.com/pingcap/failpoint v0.0.0-20240528011301-b51a646c7c86 // indirect
	github.com/pingcap/kvproto v0.0.0-20260408021215-335c5c64af53 // indirect
	github.com/pingcap/log v1.1.1-0.20250917021125-19901e015dc9 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/power-devops/perfstat v0.0.0-20221212215047-62379fc7944b // indirect
	github.com/prometheus/client_golang v1.20.5 // indirect
	github.com/prometheus/client_model v0.6.2 // indirect
	github.com/prometheus/common v0.57.0 // indirect
	github.com/prometheus/procfs v0.15.1 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20230129092748-24d4a6f8daec // indirect
	github.com/shirou/gopsutil/v3 v3.24.5 // indirect
	github.com/tikv/client-go/v2 v2.0.8-0.20260403030309-a888f4238c9b // indirect
	github.com/tikv/pd/client v0.0.0-20260401072359-048f0d8f6f71 // indirect
	github.com/yusufpapurcu/wmi v1.2.4 // indirect
	go.opencensus.io v0.24.0 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	golang.org/x/oauth2 v0.36.0
	golang.org/x/sync v0.20.0 // indirect
	golang.org/x/sys v0.42.0 // indirect
	golang.org/x/term v0.41.0 // indirect
	golang.org/x/text v0.35.0 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20260401024825-9d38bb4040a9 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260401024825-9d38bb4040a9 // indirect
	gopkg.in/natefinch/lumberjack.v2 v2.2.1 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
