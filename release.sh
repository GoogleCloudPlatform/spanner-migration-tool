#!/usr/bin/env bash

set -e

if [ $# -eq 0 ]
then
	echo "No version supplied. Run \`make release VERSION=X.X\`"
	exit 1
fi

version=$1

if ! echo $version | grep -Eq "^[0-9]+\.[0-9]+\.[0-9]+$"
then
	echo "Invalid version: ${version}"
	echo "Please specify a semantic version with no prefix (e.g. X.X.X)."
	exit 1
fi
echo "Version is semantically valid, please ensure that it is the next version as per SemVer..."

echo "Verifying that all tests pass..."
make test

echo "Verifying that spanner-migration-tool binary can be built..."
make build-static

echo "Creating Github tag v${version}"
git tag "v${version}"
git push origin "v${version}"
echo "Github tag v${version} created"

echo "Create a new release by visiting https://github.com/cloudspannerecosystem/harbourbridge/releases/new?tag=v${version}&title=v${version}"