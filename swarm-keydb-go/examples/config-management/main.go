package main

import (
	"context"
	"fmt"

	swarmkeydb "github.com/scholtz/swarm-keydb/swarm-keydb-go"
)

func main() {
	ctx := context.Background()
	client := swarmkeydb.New(swarmkeydb.Options{Host: "127.0.0.1", Port: 6379})
	_ = client.BatchPut(ctx, map[string]string{"config:theme": "dark", "config:region": "eu-west-1"})
	values, _ := client.BatchGet(ctx, []string{"config:theme", "config:region"})
	fmt.Println(values)
}
