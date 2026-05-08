package main

import (
	"context"
	"fmt"

	swarmkeydb "github.com/scholtz/swarm-keydb/swarm-keydb-go"
)

func main() {
	ctx := context.Background()
	client := swarmkeydb.New(swarmkeydb.Options{Host: "127.0.0.1", Port: 6379})
	_ = client.Put(ctx, "user:42", `{"name":"Ada","role":"admin"}`)
	value, err := client.Get(ctx, "user:42")
	if err != nil {
		fmt.Printf("failed to read user:42: %v\n", err)
		return
	}

	fmt.Println(value)
}
