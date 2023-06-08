package main

import (
	"context"
	"fmt"
)

func main() {
	ctx := context.Background()

	err := CreateSpannerConn(ctx)
	if err != nil {
		fmt.Printf("createSpannerConn(...) Error: %v", err)
	}
	err = CreateMySQLConn(ctx)
	if err != nil {
		fmt.Printf("createMySQLConn(...) Error: %v", err)
	}
	commitID, err := CreateConvWorkspace(ctx)
	if err != nil {
		fmt.Printf("createWorkspace(...) Error: %v", err)
	}
	fmt.Printf("commitId=%v, err=%v", commitID, err)
	err = CreateJob(ctx, commitID)
	if err != nil {
		fmt.Printf("createJob(...) Error: %v", err)
	}
}
