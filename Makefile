include ui.mk
# Set GOPATH env variable if not set
ifndef $(GOPATH)
    GOPATH=$(shell go env GOPATH)
    export GOPATH
endif
# Build the default binary
build: ui/package-lock.json
	cd ui/ && ng build
	go build -o spanner-migration-tool
# Build a static binary
build-static: ui/package-lock.json
	cd ui/ && ng build
	go build -a -tags osusergo,netgo -ldflags '-w -extldflags "-static"' -o spanner-migration-tool main.go
# Create a new release for Spanner migration tool.
release:
	./release.sh ${VERSION}
# Update vendor dependencies
update-vendor: ui/package-lock.json
	go mod tidy
	go mod vendor
# 	vendor non-go files
	go install github.com/goware/modvendor@latest
	$(GOPATH)/bin/modvendor -copy="**/*.c **/*.h **/*.proto" -v
	git add -u
	git commit -m "Update Vendor files" --no-edit
# Run unit tests
test:
	cd ui/ && npm install && ng build && npm test -- --browsers=ChromeHeadless --watch=false
	go test -v ./...
# Run code coverage with unit tests
test-coverage:
	cd ui/ && npm install && ng build && npm test -- --browsers=ChromeHeadless --watch=false
	go test ./... -coverprofile coverage.out -covermode count
	go tool cover -func coverage.out
