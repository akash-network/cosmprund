package cmd

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"cosmossdk.io/log"
	storetypes "cosmossdk.io/store/types"
	evidencetypes "cosmossdk.io/x/evidence/types"
	feegrant "cosmossdk.io/x/feegrant"
	upgradetypes "cosmossdk.io/x/upgrade/types"
	tmdb "github.com/cometbft/cometbft-db"
	"github.com/cometbft/cometbft/state"
	tmstore "github.com/cometbft/cometbft/store"
	db "github.com/cosmos/cosmos-db"
	authtypes "github.com/cosmos/cosmos-sdk/x/auth/types"
	authzkeeper "github.com/cosmos/cosmos-sdk/x/authz/keeper"
	banktypes "github.com/cosmos/cosmos-sdk/x/bank/types"
	distrtypes "github.com/cosmos/cosmos-sdk/x/distribution/types"
	govtypes "github.com/cosmos/cosmos-sdk/x/gov/types"
	minttypes "github.com/cosmos/cosmos-sdk/x/mint/types"
	paramtypes "github.com/cosmos/cosmos-sdk/x/params/types"
	slashingtypes "github.com/cosmos/cosmos-sdk/x/slashing/types"
	stakingtypes "github.com/cosmos/cosmos-sdk/x/staking/types"
	gogotypes "github.com/cosmos/gogoproto/types"
	iavltree "github.com/cosmos/iavl"
	iavldb "github.com/cosmos/iavl/db"
	ibctransfertypes "github.com/cosmos/ibc-go/v8/modules/apps/transfer/types"
	"github.com/spf13/cobra"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

// load db
// load app store and prune
// if immutable tree is not deletable we should import and export current state

func pruneCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "prune [path_to_home]",
		Short: "prune data from the application store and block store",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			var err error

			// Run Tendermint pruning first (if enabled)
			if tendermint {
				if err = pruneTMData(args[0]); err != nil {
					return err
				}
			}

			// Then run application state pruning (if enabled)
			if cosmosSdk {
				err = pruneAppState(args[0])
				if err != nil {
					return err
				}
			}

			return nil
		},
	}
	return cmd
}

func pruneAppState(home string) error {

	// this has the potential to expand size, should just use state sync
	// dbType := db.BackendType(backend)

	dbDir := rootify(dataDir, home)

	o := opt.Options{
		DisableSeeksCompaction: true,
	}

	// Get BlockStore
	appDB, err := db.NewGoLevelDBWithOpts("application", dbDir, &o)
	if err != nil {
		return err
	}

	fmt.Println("pruning application state")

	fmt.Println("\n=== Application State Pruning ===")

	latestHeight := getLatestVersion(appDB)
	fmt.Printf("Latest height: %d\n", latestHeight)

	if latestHeight <= 0 {
		return fmt.Errorf("database has no valid heights to prune, latest height: %d", latestHeight)
	}

	// Use the --versions flag value
	keepVersions := versions
	if keepVersions == 0 {
		keepVersions = 10
	}

	// Mount all keys for Akash Network
	keys := storetypes.NewKVStoreKeys(
		// Core SDK modules
		authtypes.StoreKey, banktypes.StoreKey, stakingtypes.StoreKey, "crisis",
		minttypes.StoreKey, distrtypes.StoreKey, slashingtypes.StoreKey,
		govtypes.StoreKey, paramtypes.StoreKey, "ibc", upgradetypes.StoreKey, feegrant.StoreKey,
		evidencetypes.StoreKey, ibctransfertypes.StoreKey,
		authzkeeper.StoreKey,
		"consensus",
		// Akash Network modules
		"escrow", "deployment", "market", "provider", "audit", "cert", "take",
	)

	fmt.Printf("Target: Keep last %d versions\n", keepVersions)
	fmt.Printf("Processing %d stores...\n\n", len(keys))

	// Process each store independently
	successCount := 0
	errorCount := 0

	for _, storeKey := range keys {
		fmt.Printf("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")
		fmt.Printf("Store: %s\n", storeKey.Name())

		storePrefix := fmt.Sprintf("s/k:%s/", storeKey.Name())
		cosmosdbPrefix := db.NewPrefixDB(appDB, []byte(storePrefix))
		wrappedDB := iavldb.NewWrapper(cosmosdbPrefix)

		logger := log.NewNopLogger()
		mutableTree := iavltree.NewMutableTree(
			wrappedDB,
			1000000,
			false,
			logger,
			iavltree.SyncOption(true),
			iavltree.AsyncPruningOption(false),
		)

		_, err := mutableTree.Load()
		if err != nil {
			fmt.Printf("  ⚠️  Skipping (cannot load): %v\n\n", err)
			errorCount++
			continue
		}

		versions := mutableTree.AvailableVersions()
		if len(versions) == 0 {
			fmt.Printf("  No versions\n\n")
			successCount++
			continue
		}

		if len(versions) <= int(keepVersions) {
			fmt.Printf("  Only %d versions, nothing to prune\n\n", len(versions))
			successCount++
			continue
		}

		firstVersion := int64(versions[0])
		lastVersion := int64(versions[len(versions)-1])
		pruneToVersion := lastVersion - int64(keepVersions)

		fmt.Printf("  Versions: %d (range: %d-%d)\n", len(versions), firstVersion, lastVersion)
		fmt.Printf("  Deleting versions up to %d (keeping last %d)\n", pruneToVersion, keepVersions)

		err = mutableTree.DeleteVersionsTo(pruneToVersion)
		if err != nil {
			fmt.Printf("  ⚠️  Error: %v\n\n", err)
			errorCount++
			continue
		}

		versionsAfter := mutableTree.AvailableVersions()
		fmt.Printf("  ✓ Deleted %d versions (%d remaining)\n\n", len(versions)-len(versionsAfter), len(versionsAfter))
		successCount++
	}

	fmt.Printf("\nSummary: %d stores pruned, %d errors\n\n", successCount, errorCount)

	// Clean up old commit info metadata
	fmt.Println("=== Cleaning Up Commit Info Metadata ===")

	// Find all commit info keys (s/<version>)
	metadataPrefix := []byte("s/")
	iter, err := appDB.Iterator(metadataPrefix, nil)
	if err != nil {
		return fmt.Errorf("failed to create metadata iterator: %w", err)
	}

	oldCommitInfoVersions := []int64{}
	for iter.Valid() {
		keyBytes := iter.Key()
		keyStr := string(keyBytes)

		// Skip if it's not our prefix
		if !strings.HasPrefix(keyStr, "s/") {
			break
		}

		// Skip store keys, latest, pruneheights
		if strings.HasPrefix(keyStr, "s/k:") || keyStr == "s/latest" || keyStr == "s/pruneheights" {
			iter.Next()
			continue
		}

		// Try to parse as a version number (s/<version>)
		var version int64
		_, err := fmt.Sscanf(keyStr[2:], "%d", &version)
		if err == nil {
			// Check if this version is older than what we're keeping
			targetVersion := latestHeight - int64(keepVersions)
			if version < targetVersion {
				oldCommitInfoVersions = append(oldCommitInfoVersions, version)
			}
		}

		iter.Next()
	}
	iter.Close()

	// Delete old commit info entries in batches
	if len(oldCommitInfoVersions) > 0 {
		fmt.Printf("Found %d old commit info entries to delete\n", len(oldCommitInfoVersions))
		fmt.Printf("Deleting commit info for versions < %d...\n", latestHeight-int64(keepVersions))

		batch := appDB.NewBatch()
		batchCount := 0
		totalDeleted := 0

		for _, version := range oldCommitInfoVersions {
			commitKey := fmt.Sprintf("s/%d", version)
			batch.Delete([]byte(commitKey))
			batchCount++
			totalDeleted++

			// Write batch every 10000 entries
			if batchCount >= 10000 {
				err = batch.Write()
				if err != nil {
					fmt.Printf("  ⚠️  ERROR writing batch: %v\n", err)
				} else {
					if totalDeleted%100000 == 0 {
						fmt.Printf("  Deleted %d commit info entries...\n", totalDeleted)
					}
				}
				batch.Close()
				batch = appDB.NewBatch()
				batchCount = 0
			}
		}

		// Write remaining
		if batchCount > 0 {
			err = batch.Write()
			if err != nil {
				fmt.Printf("  ⚠️  ERROR writing final batch: %v\n", err)
			}
			batch.Close()
		}

		fmt.Printf("✓ Deleted %d old commit info entries\n", totalDeleted)
	} else {
		fmt.Println("No old commit info entries to delete")
	}

	// Close and reopen database for compaction
	appDB.Close()

	appDB, err = db.NewGoLevelDBWithOpts("application", dbDir, &o)
	if err != nil {
		return fmt.Errorf("failed to reopen database: %w", err)
	}
	defer appDB.Close()

	fmt.Println("\nCompacting database (this may take several minutes)...")
	if err := appDB.ForceCompact(nil, nil); err != nil {
		return fmt.Errorf("compaction failed: %w", err)
	}
	fmt.Println("✓ Compaction complete")
	fmt.Println()

	return nil
}

// pruneTMData prunes the CometBFT blocks and state based on the amount of blocks to keep
func pruneTMData(home string) error {
	fmt.Println("\n=== Pruning CometBFT Data ===")

	dbDir := rootify(dataDir, home)

	// Get BlockStore (CometBFT uses cometbft-db)
	blockStoreDB, err := tmdb.NewGoLevelDB("blockstore", dbDir)
	if err != nil {
		return err
	}
	defer blockStoreDB.Close()

	blockStore := tmstore.NewBlockStore(blockStoreDB)

	// Get StateStore (CometBFT uses cometbft-db)
	stateDB, err := tmdb.NewGoLevelDB("state", dbDir)
	if err != nil {
		return err
	}
	defer stateDB.Close()

	stateStore := state.NewStore(stateDB, state.StoreOptions{})

	base := blockStore.Base()
	height := blockStore.Height()
	pruneHeight := height - int64(blocks)
	
	fmt.Printf("Block range: %d - %d (total: %d blocks)\n", base, height, height-base+1)

	needsPruning := pruneHeight > base
	if needsPruning {
		fmt.Printf("Pruning blocks up to %d (keeping last %d blocks)\n\n", pruneHeight, blocks)
	} else {
		fmt.Printf("Blocks already pruned to %d blocks, skipping pruning\n\n", blocks)
	}

	var newBase int64
	
	if needsPruning {
		// Load current state for PruneBlocks requirement
		currentState, err := stateStore.Load()
		if err != nil {
			return fmt.Errorf("failed to load state: %w", err)
		}

		// Prune blocks
		fmt.Println("Pruning block store...")
		pruned, nb, err := blockStore.PruneBlocks(pruneHeight, currentState)
		if err != nil {
			return fmt.Errorf("block pruning failed: %w", err)
		}
		newBase = nb
		fmt.Printf("  ✓ Pruned %d blocks (new base: %d)\n", pruned, newBase)

		// Prune state (evidenceThresholdHeight = base since we're pruning everything below pruneHeight)
		fmt.Println("Pruning state store...")
		err = stateStore.PruneStates(base, pruneHeight, base)
		if err != nil {
			return fmt.Errorf("state pruning failed: %w", err)
		}
		fmt.Printf("  ✓ Pruned states %d to %d\n", base, pruneHeight)
	} else {
		newBase = base
	}

	// CRITICAL: Clean up orphaned blockstore entries
	// PruneBlocks() only updates base/height but doesn't delete C:/H: keys
	fmt.Println("\nCleaning up orphaned blockstore entries...")
	
	// The valid height range is newBase to height
	fmt.Printf("  Valid height range: %d to %d\n", newBase, height)
	
	batch := blockStoreDB.NewBatch()
	deletedCount := 0
	keptCount := 0
	checkedCount := 0
	
	// Clean up all keys with height outside our range
	iter, err := blockStoreDB.Iterator(nil, nil)
	if err == nil {
		for iter.Valid() {
			key := iter.Key()
			keyStr := string(key)
			
			// Parse height from C: and H: keys (heights are ASCII strings!)
			shouldDelete := false
			if len(keyStr) >= 3 && (keyStr[:2] == "C:" || keyStr[:2] == "H:") {
				// Height is encoded as ASCII decimal string after prefix
				var blockHeight int64
				_, err := fmt.Sscanf(keyStr[2:], "%d", &blockHeight)
				if err == nil {
					// Delete if height is outside our keep range
					if blockHeight < newBase || blockHeight > height {
						shouldDelete = true
					}
				}
			}
			
			if shouldDelete {
				batch.Delete(key)
				deletedCount++
			} else {
				keptCount++
			}
			
			checkedCount++
			if checkedCount%100000 == 0 {
				fmt.Printf("\r  Checked %d entries: keeping %d, deleting %d...", checkedCount, keptCount, deletedCount)
			}
			
			// Write batch periodically
			if deletedCount > 0 && deletedCount%50000 == 0 {
				err := batch.Write()
				if err != nil {
					fmt.Printf("\nError writing batch: %v\n", err)
				}
				batch.Close()
				batch = blockStoreDB.NewBatch()
			}
			
			iter.Next()
		}
		iter.Close()
		
		// Write final batch
		if deletedCount > 0 {
			err := batch.Write()
			if err != nil {
				fmt.Printf("\nError writing final batch: %v\n", err)
			}
		}
		batch.Close()
		
		fmt.Printf("\r  ✓ Deleted %d orphaned entries (kept %d valid)\n", deletedCount, keptCount)
	}

	// Auto-compact databases to reclaim space
	fmt.Println("\nCompacting blockstore database...")
	blockStoreDB.Close()
	if err := compactTmDB("blockstore", dbDir); err != nil {
		fmt.Printf("  ⚠️  Compaction failed: %v\n", err)
	} else {
		fmt.Println("  ✓ Blockstore compacted")
	}
	
	fmt.Println("Compacting state database...")
	stateDB.Close()
	if err := compactTmDB("state", dbDir); err != nil {
		fmt.Printf("  ⚠️  Compaction failed: %v\n", err)
	} else {
		fmt.Println("  ✓ State compacted")
	}
	
	fmt.Println()
	fmt.Println("✓ CometBFT pruning and compaction complete")
	fmt.Println()

	return nil
}

// Utils

func rootify(path, root string) string {
	if filepath.IsAbs(path) {
		return path
	}
	return filepath.Join(root, path)
}

func hasPrefix(s, prefix []byte) bool {
	return len(s) >= len(prefix) && string(s[:len(prefix)]) == string(prefix)
}

func getLatestVersion(db db.DB) int64 {
	bz, err := db.Get([]byte("s/latest"))
	if err != nil {
		panic(err)
	} else if bz == nil {
		return 0
	}

	var latestVersion int64
	if err := gogotypes.StdInt64Unmarshal(&latestVersion, bz); err != nil {
		panic(err)
	}

	return latestVersion
}

func compactTmDB(name, dir string) error {
	// Open source database
	dbOld, err := tmdb.NewGoLevelDB(name, dir)
	if err != nil {
		return err
	}
	
	// Get all keys and values
	iter, err := dbOld.Iterator(nil, nil)
	if err != nil {
		dbOld.Close()
		return err
	}
	
	type kv struct {
		key   []byte
		value []byte
	}
	var entries []kv
	
	for iter.Valid() {
		key := make([]byte, len(iter.Key()))
		value := make([]byte, len(iter.Value()))
		copy(key, iter.Key())
		copy(value, iter.Value())
		entries = append(entries, kv{key, value})
		iter.Next()
	}
	iter.Close()
	dbOld.Close()
	
	// Delete old database directory
	dbPath := filepath.Join(dir, name+".db")
	if err := os.RemoveAll(dbPath); err != nil {
		return fmt.Errorf("failed to remove old db: %w", err)
	}
	
	// Create new database with fresh files
	dbNew, err := tmdb.NewGoLevelDB(name, dir)
	if err != nil {
		return err
	}
	defer dbNew.Close()
	
	// Write all entries in batches
	batch := dbNew.NewBatch()
	for i, entry := range entries {
		batch.Set(entry.key, entry.value)
		
		if (i+1)%10000 == 0 {
			if err := batch.Write(); err != nil {
				return err
			}
			batch.Close()
			batch = dbNew.NewBatch()
		}
	}
	
	// Write remaining
	if err := batch.Write(); err != nil {
		return err
	}
	batch.Close()
	
	return nil
}
