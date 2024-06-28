# Set GOPATH env variable if not set
ifndef $(GOPATH)
    GOPATH=$(shell go env GOPATH)
    export GOPATH
endif
# Build the default binary
build:
	cd ui/ && ng build
	go build -o spanner-migration-tool
# Build a static binary
build-static:
	go build -a -tags osusergo,netgo -ldflags '-w -extldflags "-static"' -o spanner-migration-tool main.go
# Create a new release for Spanner migration tool.
release:
	./release.sh ${VERSION}
# Update vendor dependencies
update-vendor:
	go mod tidy
	go mod vendor
# 	vendor non-go files
	go install github.com/goware/modvendor@latest
	$(GOPATH)/bin/modvendor -copy="**/*.c **/*.h **/*.proto" -v
	git add -u
	git commit -m "Update Vendor files" --no-edit
# Run unit tests
test:
	go test -v ./...
	cd ui/ && npm test -- --browsers=ChromeHeadless --watch=false
# Run code coverage with unit tests
test-coverage:
	go test ./... -coverprofile coverage.out -covermode count
	go tool cover -func coverage.out
	cd ui/ && npm test -- --browsers=ChromeHeadless --watch=false
