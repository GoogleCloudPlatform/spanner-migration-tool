# Build the default binary
build:
	go build
# Build a static binary
build-static:
	go build -a -tags osusergo,netgo -ldflags '-w -extldflags "-static"' -o harbourbridge main.go
# Run unit tests
test:
	go test -v ./...
# Run code coverage with unit tests
test-coverage:
	go test ./... -coverprofile coverage.out -covermode count
	go tool cover -func coverage.out
