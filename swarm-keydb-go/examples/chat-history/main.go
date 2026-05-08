package main

import (
	"context"
	"fmt"

	swarmkeydb "github.com/scholtz/swarm-keydb/swarm-keydb-go"
)

func main() {
	ctx := context.Background()
	client := swarmkeydb.New(swarmkeydb.Options{Host: "127.0.0.1", Port: 6379})
	_ = client.SetWithTTL(ctx, "chat:room1:last", `{"from":"alice","text":"hi"}`, 60)
	keys, _ := client.List(ctx, "chat:*")
	fmt.Println(keys)
}
