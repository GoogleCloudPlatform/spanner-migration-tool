# `google.golang.org/grpc/naming` patch

## What is this?
This is a copy of the contents of the `google.golang.org/grpc/naming` package as of [commit 06bc4d0c03eec227240ec67d0767b1f83e80d1d5](https://github.com/grpc/grpc-go/tree/06bc4d0c03eec227240ec67d0767b1f83e80d1d5/naming).

It is exposed as a module and declares its path to be `google.golang.org/grpc/naming`.

## What is the motivation behind this?

We need this because of a 'dependency hell' situation:

1. `grpc-go` makes breaking changes between minor releases of their module.
2. tidb v1.1.0-beta [depends on a `grpc/naming` API which was removed in v1.30](https://github.com/etcd-io/etcd/issues/12124). We depend on etcd v0.5.0-alpha indirectly via tidb v1.1.0-beta.

This situation would be solved by bumping tidb to version which depends on etcd v3.6.0+, which does not make use of the removed grpc-go API.

This module is used to solve this dependency hell, by adding the `google.golang.org/grpc/naming` package into version v1.44 of grpc-go.

It is the smallest patch that can be applied to solve this particular 'dependency hell' issue with grpc-go. It will not fix other issues when packages depend on other, newer packages.

## When can it be removed?

This must be removed when tidb does not need grpc/naming.