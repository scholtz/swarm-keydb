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
	value, _ := client.Get(ctx, "user:42")
	fmt.Println(value)
}
