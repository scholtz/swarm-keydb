// Package main demonstrates Redis Streams with SwarmKeyDb.
package main

import (
	"context"
	"fmt"
	"log"

	swarmkeydb "github.com/scholtz/swarm-keydb/sdk/go"
)

func main() {
	client := swarmkeydb.NewClient(&swarmkeydb.Options{
		Addr: "ws://localhost:8765",
	})
	defer client.Close()

	ctx := context.Background()
	stream := "events"

	// Add entries to the stream
	for i := 1; i <= 5; i++ {
		id, err := client.XAdd(ctx, &swarmkeydb.XAddArgs{
			Stream: stream,
			Values: []interface{}{"event", fmt.Sprintf("event-%d", i), "seq", fmt.Sprintf("%d", i)},
		}).Result()
		if err != nil {
			log.Fatalf("XAdd error: %v", err)
		}
		fmt.Printf("XAdd -> %s\n", id)
	}

	// Get stream length
	length, err := client.XLen(ctx, stream).Result()
	if err != nil {
		log.Fatalf("XLen error: %v", err)
	}
	fmt.Printf("Stream length: %d\n", length)

	// Read all entries
	msgs, err := client.XRange(ctx, stream, "-", "+").Result()
	if err != nil {
		log.Fatalf("XRange error: %v", err)
	}
	fmt.Printf("XRange returned %d messages:\n", len(msgs))
	for _, m := range msgs {
		fmt.Printf("  %s: %v\n", m.ID, m.Values)
	}

	// Create consumer group
	if err := client.XGroupCreate(ctx, stream, "mygroup", "0").Err(); err != nil {
		log.Printf("XGroupCreate (may already exist): %v", err)
	}

	// Read via consumer group
	streams, err := client.XReadGroup(ctx, &swarmkeydb.XReadGroupArgs{
		Group:    "mygroup",
		Consumer: "consumer-1",
		Streams:  []string{stream, ">"},
		Count:    10,
		Block:    -1,
	}).Result()
	if err != nil {
		log.Fatalf("XReadGroup error: %v", err)
	}
	fmt.Printf("XReadGroup returned %d streams\n", len(streams))
	for _, s := range streams {
		for _, m := range s.Messages {
			fmt.Printf("  ACKing %s\n", m.ID)
			if err := client.XAck(ctx, stream, "mygroup", m.ID).Err(); err != nil {
				log.Printf("XAck error: %v", err)
			}
		}
	}
}
