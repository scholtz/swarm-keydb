// Package main demonstrates pub/sub with SwarmKeyDb.
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
		Addr: "ws://localhost:8765",
	})
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Subscribe to a channel
	ps, err := client.Subscribe(ctx, "news")
	if err != nil {
		log.Fatalf("Subscribe error: %v", err)
	}
	defer ps.Close()

	ch := ps.Channel()

	// Publish from another goroutine
	go func() {
		time.Sleep(500 * time.Millisecond)
		for i := 1; i <= 3; i++ {
			msg := fmt.Sprintf("Breaking news #%d", i)
			if err := client.Publish(ctx, "news", msg).Err(); err != nil {
				log.Printf("Publish error: %v", err)
				return
			}
			fmt.Printf("Published: %s\n", msg)
			time.Sleep(200 * time.Millisecond)
		}
	}()

	received := 0
	for {
		select {
		case msg, ok := <-ch:
			if !ok {
				return
			}
			fmt.Printf("Received on %s: %s\n", msg.Channel, msg.Payload)
			received++
			if received >= 3 {
				fmt.Println("Done!")
				return
			}
		case <-ctx.Done():
			fmt.Println("Timeout")
			return
		}
	}
}
