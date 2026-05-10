// Package main demonstrates optimistic locking transactions with SwarmKeyDb.
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

	// Initialize counter
	if err := client.Set(ctx, "txcounter", "0", 0).Err(); err != nil {
		log.Fatalf("Set error: %v", err)
	}

	// Optimistic increment using Watch
	err := client.Watch(ctx, func(tx *swarmkeydb.Tx) error {
		_, err := tx.TxPipelined(ctx, func(pipe swarmkeydb.Pipeliner) error {
			pipe.Do(ctx, "INCR", "txcounter")
			return nil
		})
		return err
	}, "txcounter")

	if err != nil {
		log.Fatalf("Watch error: %v", err)
	}

	val, err := client.Get(ctx, "txcounter").Result()
	if err != nil {
		log.Fatalf("Get error: %v", err)
	}
	fmt.Printf("txcounter = %s\n", val)

	// TxPipelined (MULTI/EXEC) without Watch
	results, err := client.TxPipelined(ctx, func(pipe swarmkeydb.Pipeliner) error {
		pipe.Do(ctx, "SET", "pipeline-key", "pipeline-value")
		pipe.Do(ctx, "GET", "pipeline-key")
		return nil
	})
	if err != nil {
		log.Fatalf("TxPipelined error: %v", err)
	}
	fmt.Printf("TxPipelined results: %d commands executed\n", len(results))
	for i, r := range results {
		if cmd, ok := r.(*swarmkeydb.Cmd); ok {
			v, _ := cmd.Result()
			fmt.Printf("  [%d] -> %v\n", i, v)
		}
	}
}
