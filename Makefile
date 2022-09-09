# Set GOPATH env variable if not set
ifndef $(GOPATH)
    GOPATH=$(shell go env GOPATH)
    export GOPATH
endif
# Build the default binary
build:
	go build
# Build a static binary
build-static:
	go build -a -tags osusergo,netgo -ldflags '-w -extldflags "-static"' -o harbourbridge main.go
# Build the default binary with vendored dependencies
build-vendor:
	go mod tidy
# 	vendor the dependencies
	go mod vendor
# 	vendor non-go files
	go install github.com/goware/modvendor@latest
	$(GOPATH)/bin/modvendor -copy="**/*.c **/*.h **/*.proto" -v
# 	build the binary
	go build
# Build the static binary with vendored dependencies
build-static-vendor:
	go mod tidy
# 	vendor the dependencies
	go mod vendor
# 	vendor non-go files
	go install github.com/goware/modvendor@latest
	$(GOPATH)/bin/modvendor -copy="**/*.c **/*.h **/*.proto" -v
# build the binary
	go build -a -tags osusergo,netgo -ldflags '-w -extldflags "-static"' -o harbourbridge main.go
# Run unit tests
test:
	go test -v ./...
# Run code coverage with unit tests
test-coverage:
	go test ./... -coverprofile coverage.out -covermode count
	go tool cover -func coverage.out

ng-build:
	cd ./ui && npm install && npm run build

run-binary:
	./harbourbridge -webv2

run-all:	ng-build build run-binary