// Package main demonstrates basic key-value operations with SwarmKeyDb.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	swarmkeydb "github.com/scholtz/swarm-keydb/sdk/go"
)

func main() {
	client := swarmkeydb.NewClient(&swarmkeydb.Options{
		Addr:     "ws://localhost:8765",
		HTTPAddr: "http://localhost:8080",
	})
	defer client.Close()

	ctx := context.Background()

	// Set a key with a 60-second TTL
	if err := client.Set(ctx, "greeting", "Hello, SwarmKeyDb!", 60*time.Second).Err(); err != nil {
		log.Fatalf("Set error: %v", err)
	}
	fmt.Println("Set greeting -> OK")

	// Get the key
	val, err := client.Get(ctx, "greeting").Result()
	if err != nil {
		log.Fatalf("Get error: %v", err)
	}
	fmt.Printf("Get greeting -> %s\n", val)

	// TTL
	ttl, err := client.TTL(ctx, "greeting").Result()
	if err != nil {
		log.Fatalf("TTL error: %v", err)
	}
	fmt.Printf("TTL greeting -> %s\n", ttl)

	// Increment a counter
	for i := 0; i < 5; i++ {
		n, err := client.Incr(ctx, "counter").Result()
		if err != nil {
			log.Fatalf("Incr error: %v", err)
		}
		fmt.Printf("counter = %d\n", n)
	}

	// Delete keys
	deleted, err := client.Del(ctx, "greeting", "counter").Result()
	if err != nil {
		log.Fatalf("Del error: %v", err)
	}
	fmt.Printf("Deleted %d keys\n", deleted)
}
