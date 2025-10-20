package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/syndtr/goleveldb/leveldb/opt"
	db "github.com/cosmos/cosmos-db"

	"github.com/binaryholdings/cosmos-pruner/internal/rootmulti"
)

func checkCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "check [path_to_home]",
		Short: "check the latest version/height of stores without pruning",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			dbDir := rootify(dataDir, args[0])

			o := opt.Options{
				DisableSeeksCompaction: true,
			}

			fmt.Println("=== Database Version Check ===")
			fmt.Println("Data directory:", dbDir)
			fmt.Println()

			// Check Application DB
			fmt.Println("1. Application Store:")
			appDB, err := db.NewGoLevelDBWithOpts("application", dbDir, &o)
			if err != nil {
				return fmt.Errorf("failed to open application DB: %w", err)
			}
			defer appDB.Close()

			appVersion := rootmulti.GetLatestVersion(appDB)
			fmt.Printf("   Latest version: %d\n", appVersion)
			fmt.Println()

			// Check BlockStore DB
			fmt.Println("2. Block Store:")
			blockDB, err := db.NewGoLevelDBWithOpts("blockstore", dbDir, &o)
			if err != nil {
				return fmt.Errorf("failed to open blockstore DB: %w", err)
			}
			defer blockDB.Close()

			// Read block store metadata
			blockStoreStateBytes, err := blockDB.Get([]byte("blockStore"))
			if err != nil {
				fmt.Println("   Could not read blockstore state")
			} else if blockStoreStateBytes != nil {
				// The blockstore state contains base and height as int64 values
				// For simplicity, just indicate we found it
				fmt.Println("   Blockstore state found")
			}

			// Try to get block store base/height another way
			baseBytes, _ := blockDB.Get([]byte("H:base"))
			heightBytes, _ := blockDB.Get([]byte("H:height"))
			
			if len(baseBytes) == 8 {
				fmt.Printf("   Base found in DB\n")
			}
			if len(heightBytes) == 8 {
				fmt.Printf("   Height found in DB\n")
			}
			
			fmt.Println("   (Use RPC to get exact block heights)")
			fmt.Println()

			// Check State DB
			fmt.Println("3. State Store:")
			stateDB, err := db.NewGoLevelDBWithOpts("state", dbDir, &o)
			if err != nil {
				return fmt.Errorf("failed to open state DB: %w", err)
			}
			defer stateDB.Close()

			fmt.Println("   State DB opened successfully")
			fmt.Println()

			// Summary
			fmt.Println("=== SUMMARY ===")
			fmt.Printf("Application Store Height: %d\n", appVersion)
			fmt.Println()
			
			fmt.Println("To compare with blockchain height, run:")
			fmt.Println("  curl -s http://localhost:26657/status | jq -r '.result.sync_info.latest_block_height'")
			fmt.Println()
			
			if appVersion > 0 {
				fmt.Println("If there's a large gap between app store height and blockchain height,")
				fmt.Println("your node's application is not processing blocks properly.")
			}

			return nil
		},
	}
	return cmd
}

