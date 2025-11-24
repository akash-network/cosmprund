package cmd

import (
	"encoding/binary"
	"fmt"
	"path/filepath"
	"strings"

	"cosmossdk.io/log"
	storetypes "cosmossdk.io/store/types"
	"github.com/cometbft/cometbft/state"
	tmstore "github.com/cometbft/cometbft/store"
	tmdb "github.com/cometbft/cometbft-db"
	iavltree "github.com/cosmos/iavl"
	iavldb "github.com/cosmos/iavl/db"
	authtypes "github.com/cosmos/cosmos-sdk/x/auth/types"
	authzkeeper "github.com/cosmos/cosmos-sdk/x/authz/keeper"
	banktypes "github.com/cosmos/cosmos-sdk/x/bank/types"
	paramtypes "github.com/cosmos/cosmos-sdk/x/params/types"
	distrtypes "github.com/cosmos/cosmos-sdk/x/distribution/types"
	evidencetypes "cosmossdk.io/x/evidence/types"
	feegrant "cosmossdk.io/x/feegrant"
	govtypes "github.com/cosmos/cosmos-sdk/x/gov/types"
	minttypes "github.com/cosmos/cosmos-sdk/x/mint/types"
	slashingtypes "github.com/cosmos/cosmos-sdk/x/slashing/types"
	stakingtypes "github.com/cosmos/cosmos-sdk/x/staking/types"
	upgradetypes "cosmossdk.io/x/upgrade/types"
	ibctransfertypes "github.com/cosmos/ibc-go/v8/modules/apps/transfer/types"
	"github.com/spf13/cobra"
	"github.com/syndtr/goleveldb/leveldb/opt"
	db "github.com/cosmos/cosmos-db"

	"github.com/binaryholdings/cosmos-pruner/internal/rootmulti"
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

	//TODO: need to get all versions in the store, setting randomly is too slow
	fmt.Println("pruning application state")

	// Mount all keys for Akash Network
	// Core SDK stores + Akash-specific stores
	keys := storetypes.NewKVStoreKeys(
		// Core SDK modules
		authtypes.StoreKey, banktypes.StoreKey, stakingtypes.StoreKey, "crisis",
		minttypes.StoreKey, distrtypes.StoreKey, slashingtypes.StoreKey,
		govtypes.StoreKey, paramtypes.StoreKey, "ibc", upgradetypes.StoreKey, feegrant.StoreKey,
		evidencetypes.StoreKey, ibctransfertypes.StoreKey,
		authzkeeper.StoreKey,
		"consensus", // consensus params store (new in SDK v0.53)
		// Akash Network modules
		"escrow",     // escrow.StoreKey
		"deployment", // deployment.StoreKey
		"market",     // market.StoreKey
		"provider",   // provider.StoreKey
		"audit",      // audit.StoreKey
		"cert",       // cert.StoreKey
		"take",       // take.StoreKey
	)
	

	// Prune each store independently using raw IAVL MutableTree
	// This bypasses SDK wrapper limitations and enables offline pruning
	
	fmt.Println("\n=== Pruning Application Stores ===")
	
	latestHeight := rootmulti.GetLatestVersion(appDB)
	fmt.Printf("Latest height: %d\n", latestHeight)
	
	if latestHeight <= 0 {
		return fmt.Errorf("database has no valid heights to prune, latest height: %d", latestHeight)
	}
	
	// Use the --versions flag value
	keepVersions := versions
	if keepVersions == 0 {
		keepVersions = 10
	}
	
	fmt.Printf("Keeping last %d versions\n", keepVersions)
	fmt.Printf("Processing %d stores...\n\n", len(keys))
	
	// Process each store independently
	successCount := 0
	errorCount := 0
	
	for _, storeKey := range keys {
		fmt.Printf("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")
		fmt.Printf("Store: %s\n", storeKey.Name())
		
		// Create a new store for this specific key
		appStore := rootmulti.NewStore(appDB)
		appStore.MountStoreWithDB(storeKey, storetypes.StoreTypeIAVL, nil)
		
		// Try to load this store
		err = appStore.LoadLatestVersion()
		if err != nil {
			fmt.Printf("  ⚠️  Skipping (doesn't exist or corrupted)\n\n")
			errorCount++
			continue
		}
		
		// Get all versions BEFORE pruning
		versionsBefore := appStore.GetAllVersions()
		if len(versionsBefore) == 0 {
			fmt.Printf("  No versions, skipping\n\n")
			successCount++
			continue
		}
		
		firstVersion := int64(versionsBefore[0])
		lastVersion := int64(versionsBefore[len(versionsBefore)-1])
		
		// Calculate pruning target
		if len(versionsBefore) <= int(keepVersions) {
			fmt.Printf("  Only %d versions, nothing to prune\n\n", len(versionsBefore))
			successCount++
			continue
		}
		
		// Calculate the highest version to delete (keep last N versions)
		pruneToVersion := lastVersion - int64(keepVersions)
		if pruneToVersion < firstVersion {
			fmt.Printf("  Nothing to prune\n\n")
			successCount++
			continue
		}
		
		fmt.Printf("  Versions: %d total (range: %d-%d)\n", len(versionsBefore), firstVersion, lastVersion)
		
		// Safety: Don't prune stores that are too new
		// Need at least 4x keepVersions to have meaningful pruning and safety margin
		minVersionsForPruning := int(keepVersions) * 4
		if len(versionsBefore) < minVersionsForPruning {
			fmt.Printf("  Store too new (%d versions < %d threshold), skipping for safety\n\n", len(versionsBefore), minVersionsForPruning)
			successCount++
			continue
		}
		
		fmt.Printf("  Pruning to version %d (keeping last %d)\n", pruneToVersion, keepVersions)
		
		// Use raw IAVL MutableTree with SyncOption(true) for offline pruning
		storePrefix := fmt.Sprintf("s/k:%s/", storeKey.Name())
		cosmosdbPrefix := db.NewPrefixDB(appDB, []byte(storePrefix))
		wrappedDB := iavldb.NewWrapper(cosmosdbPrefix)
		
		logger := log.NewNopLogger()
		mutableTree := iavltree.NewMutableTree(
			wrappedDB, 
			1000000, 
			false, 
			logger,
			iavltree.SyncOption(true),           // Force fsync for offline pruning
			iavltree.AsyncPruningOption(false),  // Synchronous deletion
		)
		
		_, err := mutableTree.Load()
		if err != nil {
			fmt.Printf("  ⚠️  ERROR: %v\n\n", err)
			errorCount++
			continue
		}
		
		availableVersionsBefore := mutableTree.AvailableVersions()
		if len(availableVersionsBefore) == 0 {
			fmt.Printf("  No versions\n\n")
			successCount++
			continue
		}
		
		firstVer := int64(availableVersionsBefore[0])
		
		// CRITICAL FIX: Only delete versions that actually exist in this store!
		// Don't try to delete versions before firstVer
		actualPruneToVersion := pruneToVersion
		if actualPruneToVersion < firstVer {
			// This store doesn't have versions that old
			fmt.Printf("  Store only has versions from %d onwards, nothing to prune\n\n", firstVer)
			successCount++
			continue
		}
		
		hadError := false
		isDeploymentStore := storeKey.Name() == "deployment"
		
		if isDeploymentStore {
			// For deployment store: use manual deletion (DeleteVersionsTo doesn't work well)
			fmt.Printf("  Manually deleting root hash keys and orphaned nodes...\n")
			
			prefixBytes := []byte(storePrefix)
			iter, err := appDB.Iterator(prefixBytes, nil)
			actualRootHashVersions := []int64{}
			if err == nil {
				for iter.Valid() {
					keyBytes := iter.Key()
					if !hasPrefix(keyBytes, prefixBytes) {
						iter.Next()
						continue
					}
					
					suffix := keyBytes[len(prefixBytes):]
					// Root hash keys: "r" + 8-byte big-endian version number
					if len(suffix) >= 9 && suffix[0] == 'r' {
						versionNum := int64(binary.BigEndian.Uint64(suffix[1:9]))
						actualRootHashVersions = append(actualRootHashVersions, versionNum)
					}
					iter.Next()
				}
				iter.Close()
			}
			
			batch := appDB.NewBatch()
			deletedRootHashes := 0
			deletedOrphanedNodes := 0
			
			// Delete root hash keys for versions we want to prune
			for _, version := range actualRootHashVersions {
				if version <= actualPruneToVersion {
					// Build root hash key: "s/k:deployment/r" + 8-byte version
					rootHashKey := make([]byte, len(prefixBytes)+9)
					copy(rootHashKey, prefixBytes)
					rootHashKey[len(prefixBytes)] = 'r'
					binary.BigEndian.PutUint64(rootHashKey[len(prefixBytes)+1:], uint64(version))
					
					batch.Delete(rootHashKey)
					deletedRootHashes++
				}
			}
			
			// Delete orphaned nodes ("o" keys) that reference old versions
			iter2, err := appDB.Iterator(prefixBytes, nil)
			if err == nil {
				for iter2.Valid() {
					keyBytes := iter2.Key()
					if !hasPrefix(keyBytes, prefixBytes) {
						iter2.Next()
						continue
					}
					
					suffix := keyBytes[len(prefixBytes):]
					// Orphaned node keys: "o" + 8-byte version1 + 8-byte version2 + ...
					if len(suffix) >= 17 && suffix[0] == 'o' {
						// Extract the first version from the orphaned node key
						orphanVersion1 := int64(binary.BigEndian.Uint64(suffix[1:9]))
						
						// If this orphaned node references a version we're pruning, delete it
						if orphanVersion1 <= actualPruneToVersion {
							batch.Delete(keyBytes)
							deletedOrphanedNodes++
						}
					}
					iter2.Next()
				}
				iter2.Close()
			}
			
			// Write the batch
			if deletedRootHashes > 0 || deletedOrphanedNodes > 0 {
				fmt.Printf("  Deleting %d root hash keys and %d orphaned nodes...\n", deletedRootHashes, deletedOrphanedNodes)
				err = batch.Write()
				if err != nil {
					fmt.Printf("  ⚠️  ERROR: Failed to write deletion batch: %v\n", err)
					hadError = true
				} else {
					fmt.Printf("  ✓ Deleted %d root hash keys and %d orphaned nodes\n", deletedRootHashes, deletedOrphanedNodes)
				}
			} else {
				fmt.Printf("  No keys to delete\n")
			}
			batch.Close()
		} else {
			// For other stores: use standard DeleteVersionsTo method
			fmt.Printf("  Using DeleteVersionsTo...\n")
			
			// Prune in batches
			batchSize := int64(10000)
			deletionRange := actualPruneToVersion - firstVer + 1
			totalBatches := (deletionRange + batchSize - 1) / batchSize
			if totalBatches > 1 {
				fmt.Printf("  Pruning in %d batches (deleting %d-%d)...\n", totalBatches, firstVer, actualPruneToVersion)
			}
			
			batchCount := 0
			for currentVer := firstVer; currentVer <= actualPruneToVersion; currentVer += batchSize {
				deleteUpTo := currentVer + batchSize - 1
				if deleteUpTo > actualPruneToVersion {
					deleteUpTo = actualPruneToVersion
				}
				
				batchCount++
				if totalBatches > 1 && (batchCount%5 == 1 || batchCount == int(totalBatches)) {
					fmt.Printf("  Progress: batch %d/%d (deleting to version %d)...\n", batchCount, totalBatches, deleteUpTo)
				}
				
				err = mutableTree.DeleteVersionsTo(deleteUpTo)
				if err != nil {
					errMsg := err.Error()
					if errMsg == "version does not exist" || 
					   strings.Contains(errMsg, "is less than or equal to") {
						continue
					}
					fmt.Printf("  ⚠️  Error at batch %d: %v\n", batchCount, err)
					hadError = true
					break
				}
			}
		}
		
		// Check results
		availableVersionsAfter := mutableTree.AvailableVersions()
		countBefore := len(availableVersionsBefore)
		countAfter := len(availableVersionsAfter)
		deleted := countBefore - countAfter
		
		if hadError {
			fmt.Printf("  ⚠️  Partial: deleted %d of %d versions (%d remaining)\n\n", deleted, countBefore, countAfter)
			errorCount++
			continue
		}
		
		if deleted > 0 {
			fmt.Printf("  ✓ Deleted %d versions (%d remaining)\n\n", deleted, countAfter)
		} else if deleted == 0 {
			fmt.Printf("  No versions were deleted (still %d versions)\n\n", countAfter)
		} else {
			// Unexpected increase - show diagnostics
			fmt.Printf("  ⚠️  Unexpected: version count increased (was %d, now %d)\n", countBefore, countAfter)
			if countAfter > 0 && countAfter < 10 {
				fmt.Printf("  After array: %v\n", availableVersionsAfter)
			} else if countAfter > 0 {
				fmt.Printf("  After range: %d - %d\n", availableVersionsAfter[0], availableVersionsAfter[countAfter-1])
			}
			fmt.Printf("  WARNING: This store may be corrupted, skipping\n\n")
			errorCount++
			continue
		}
		
		successCount++
	}
	
	fmt.Printf("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")
	fmt.Printf("\nSummary: %d stores pruned successfully", successCount)
	if errorCount > 0 {
		fmt.Printf(" (%d skipped)", errorCount)
	}
	fmt.Printf("\n\n")

	// Close and reopen database for compaction
	appDB.Close()
	
	appDB, err = db.NewGoLevelDBWithOpts("application", dbDir, &o)
	if err != nil {
		return fmt.Errorf("failed to reopen database: %w", err)
	}
	defer appDB.Close()

	fmt.Println("Compacting database (this may take several minutes)...")
	if err := appDB.ForceCompact(nil, nil); err != nil {
		return fmt.Errorf("compaction failed: %w", err)
	}
	fmt.Println("✓ Compaction complete\n")

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
	
	if pruneHeight <= base {
		fmt.Printf("Nothing to prune (base: %d, height: %d, keeping: %d blocks)\n", base, height, blocks)
		return nil
	}

	fmt.Printf("Block range: %d - %d\n", base, height)
	fmt.Printf("Pruning blocks up to %d (keeping last %d blocks)\n\n", pruneHeight, blocks)

	// Load current state for PruneBlocks requirement
	currentState, err := stateStore.Load()
	if err != nil {
		return fmt.Errorf("failed to load state: %w", err)
	}

	// Prune blocks
	fmt.Println("Pruning block store...")
	pruned, newBase, err := blockStore.PruneBlocks(pruneHeight, currentState)
	if err != nil {
		return fmt.Errorf("block pruning failed: %w", err)
	}
	fmt.Printf("  ✓ Pruned %d blocks (new base: %d)\n", pruned, newBase)

	// Prune state (evidenceThresholdHeight = base since we're pruning everything below pruneHeight)
	fmt.Println("Pruning state store...")
	err = stateStore.PruneStates(base, pruneHeight, base)
	if err != nil {
		return fmt.Errorf("state pruning failed: %w", err)
	}
	fmt.Printf("  ✓ Pruned states %d to %d\n", base, pruneHeight)

	// Note: cometbft-db v0.14 GoLevelDB doesn't expose compaction directly
	// Compaction will happen automatically over time
	fmt.Println("✓ CometBFT pruning complete\n")

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
