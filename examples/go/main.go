package main

import (
	"context"
	"fmt"
	"log"

	swarmkeydb "github.com/scholtz/swarm-keydb/sdk/go"
)

func main() {
	client := swarmkeydb.NewClient(&swarmkeydb.Options{Addr: "ws://127.0.0.1:8765"})
	defer client.Close()

	ctx := context.Background()
	if err := client.Set(ctx, "hello", "world", 0).Err(); err != nil {
		log.Fatal(err)
	}
	value, err := client.Get(ctx, "hello").Result()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(value)
}
