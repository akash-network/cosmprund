package cmd

import (
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

	// Mount keys from core SDK
	keys := storetypes.NewKVStoreKeys(
		authtypes.StoreKey, banktypes.StoreKey, stakingtypes.StoreKey, "crisis",
		minttypes.StoreKey, distrtypes.StoreKey, slashingtypes.StoreKey,
		govtypes.StoreKey, paramtypes.StoreKey, "ibc", upgradetypes.StoreKey, feegrant.StoreKey,
		evidencetypes.StoreKey, ibctransfertypes.StoreKey,
		authzkeeper.StoreKey,
		"consensus", // consensus params store (new in SDK v0.53)
	)

	if app == "osmosis" {
		osmoKeys := storetypes.NewKVStoreKeys(
			"icahost",        //icahosttypes.StoreKey,
			"gamm",           // gammtypes.StoreKey,
			"lockup",         //lockuptypes.StoreKey,
			"incentives",     // incentivestypes.StoreKey,
			"epochs",         // epochstypes.StoreKey,
			"poolincentives", //poolincentivestypes.StoreKey,
			"txfees",         // txfeestypes.StoreKey,
			"superfluid",     // superfluidtypes.StoreKey,
			"bech32ibc",      // bech32ibctypes.StoreKey,
			"wasm",           // wasm.StoreKey,
			"tokenfactory",   //tokenfactorytypes.StoreKey,
		)
		for key, value := range osmoKeys {
			keys[key] = value
		}
	} else if app == "cosmoshub" {
		cosmoshubKeys := storetypes.NewKVStoreKeys(
			"liquidity",
			"icahost", // icahosttypes.StoreKey
		)
		for key, value := range cosmoshubKeys {
			keys[key] = value
		}
	} else if app == "terra" { // terra classic
		terraKeys := storetypes.NewKVStoreKeys(
			"oracle",   // oracletypes.StoreKey,
			"market",   // markettypes.StoreKey,
			"treasury", //treasurytypes.StoreKey,
			"wasm",     // wasmtypes.StoreKey,
		)
		for key, value := range terraKeys {
			keys[key] = value
		}
	} else if app == "kava" {
		kavaKeys := storetypes.NewKVStoreKeys(
			"feemarket", //feemarkettypes.StoreKey,
			"kavadist",  //kavadisttypes.StoreKey,
			"auction",   //auctiontypes.StoreKey,
			"issuance",  //issuancetypes.StoreKey,
			"bep3",      //bep3types.StoreKey,
			//"pricefeed", //pricefeedtypes.StoreKey,
			//"swap",      //swaptypes.StoreKey,
			"cdp",       //cdptypes.StoreKey,
			"hard",      //hardtypes.StoreKey,
			"committee", //committeetypes.StoreKey,
			"incentive", //incentivetypes.StoreKey,
			"evmutil",   //evmutiltypes.StoreKey,
			"savings",   //savingstypes.StoreKey,
			"bridge",    //bridgetypes.StoreKey,
		)
		for key, value := range kavaKeys {
			keys[key] = value
		}

		delete(keys, "mint") // minttypes.StoreKey
	} else if app == "evmos" {
		evmosKeys := storetypes.NewKVStoreKeys(
			"evm",        // evmtypes.StoreKey,
			"feemarket",  // feemarkettypes.StoreKey,
			"inflation",  // inflationtypes.StoreKey,
			"erc20",      // erc20types.StoreKey,
			"incentives", // incentivestypes.StoreKey,
			"epochs",     // epochstypes.StoreKey,
			"claims",     // claimstypes.StoreKey,
			"vesting",    // vestingtypes.StoreKey,
		)
		for key, value := range evmosKeys {
			keys[key] = value
		}
	} else if app == "gravitybridge" {
		gravitybridgeKeys := storetypes.NewKVStoreKeys(
			"gravity",   //  gravitytypes.StoreKey,
			"bech32ibc", // bech32ibctypes.StoreKey,
		)
		for key, value := range gravitybridgeKeys {
			keys[key] = value
		}
	} else if app == "sifchain" {
		sifchainKeys := storetypes.NewKVStoreKeys(
			"dispensation",  // disptypes.StoreKey,
			"ethbridge",     // ethbridgetypes.StoreKey,
			"clp",           // clptypes.StoreKey,
			"oracle",        // oracletypes.StoreKey,
			"tokenregistry", // tokenregistrytypes.StoreKey,
			"admin",         // admintypes.StoreKey,
		)
		for key, value := range sifchainKeys {
			keys[key] = value
		}
	} else if app == "starname" {
		starnameKeys := storetypes.NewKVStoreKeys(
			"wasm",          // wasm.StoreKey,
			"configuration", // configuration.StoreKey,
			"starname",      // starname.DomainStoreKey,
		)
		for key, value := range starnameKeys {
			keys[key] = value
		}
	} else if app == "regen" {
		regenKeys := storetypes.NewKVStoreKeys()
		for key, value := range regenKeys {
			keys[key] = value
		}
	} else if app == "akash" {
		akashKeys := storetypes.NewKVStoreKeys(
			"escrow",     // escrow.StoreKey,
			"deployment", // deployment.StoreKey,
			"market",     // market.StoreKey,
			"provider",   // provider.StoreKey,
			"audit",      // audit.StoreKey,
			"cert",       // cert.StoreKey,
		)
		for key, value := range akashKeys {
			keys[key] = value
		}
	} else if app == "sentinel" {
		sentinelKeys := storetypes.NewKVStoreKeys(
			"distribution", // distributiontypes.StoreKey,
			"custommint",   // customminttypes.StoreKey,
			"swap",         // swaptypes.StoreKey,
			"vpn",          // vpntypes.StoreKey,
		)
		for key, value := range sentinelKeys {
			keys[key] = value
		}
	} else if app == "emoney" {
		emoneyKeys := storetypes.NewKVStoreKeys(
			"liquidityprovider", // lptypes.StoreKey,
			"issuer",            // issuer.StoreKey,
			"authority",         // authority.StoreKey,
			"market",            // market.StoreKey,
			//"market_indices",    // market.StoreKeyIdx,
			"buyback",   // buyback.StoreKey,
			"inflation", // inflation.StoreKey,
		)

		for key, value := range emoneyKeys {
			keys[key] = value
		}
	} else if app == "ixo" {
		ixoKeys := storetypes.NewKVStoreKeys(
			"did",      // didtypes.StoreKey,
			"bonds",    // bondstypes.StoreKey,
			"payments", // paymentstypes.StoreKey,
			"project",  // projecttypes.StoreKey,
		)

		for key, value := range ixoKeys {
			keys[key] = value
		}
	} else if app == "juno" {
		junoKeys := storetypes.NewKVStoreKeys(
			"icahost", // icahosttypes.StoreKey,
			"wasm",    // wasm.StoreKey,
		)

		for key, value := range junoKeys {
			keys[key] = value
		}
	} else if app == "likecoin" {
		likecoinKeys := storetypes.NewKVStoreKeys(
			// custom modules
			"iscn",    // iscntypes.StoreKey,
			"nft",     // nftkeeper.StoreKey,
			"likenft", // likenfttypes.StoreKey,
		)

		for key, value := range likecoinKeys {
			keys[key] = value
		}
	} else if app == "teritori" {
		// https://github.com/TERITORI/teritori-chain/blob/main/app/app.go#L323
		teritoriKeys := storetypes.NewKVStoreKeys(
			// common modules
			"packetfowardmiddleware", // routertypes.StoreKey,
			"icahost",                // icahosttypes.StoreKey,
			"wasm",                   // wasm.StoreKey,
			// custom modules
			"airdrop", // airdroptypes.StoreKey,
		)

		for key, value := range teritoriKeys {
			keys[key] = value
		}
	} else if app == "jackal" {
		// https://github.com/JackalLabs/canine-chain/blob/master/app/app.go#L347
		jackalKeys := storetypes.NewKVStoreKeys(
			// common modules
			"wasm",    // wasm.StoreKey,
			"icahost", // icahosttypes.StoreKey,
			// custom modules
			"icacontroller", // icacontrollertypes.StoreKey, https://github.com/cosmos/ibc-go/blob/main/modules/apps/27-interchain-accounts/controller/types/keys.go#L5
			// intertx is a demo and not an officially supported IBC team implementation
			"intertx",       // intertxtypes.StoreKey, https://github.com/cosmos/interchain-accounts-demo/blob/8d4683081df0e1945be40be8ac18aa182106a660/x/inter-tx/types/keys.go#L4
			"rns",           // rnsmoduletypes.StoreKey, https://github.com/JackalLabs/canine-chain/blob/master/x/rns/types/keys.go#L5
			"storage",       // storagemoduletypes.StoreKey, https://github.com/JackalLabs/canine-chain/blob/master/x/storage/types/keys.go#L5
			"dsig",          // dsigmoduletypes.StoreKey, https://github.com/JackalLabs/canine-chain/blob/master/x/dsig/types/keys.go#L5
			"filetree",      // filetreemoduletypes.StoreKey, https://github.com/JackalLabs/canine-chain/blob/master/x/filetree/types/keys.go#L5
			"notifications", // notificationsmoduletypes.StoreKey, https://github.com/JackalLabs/canine-chain/blob/master/x/notifications/types/keys.go#L5
			"jklmint",       // jklmintmoduletypes.StoreKey, https://github.com/JackalLabs/canine-chain/blob/master/x/jklmint/types/keys.go#L7
			"lp",            // lpmoduletypes.StoreKey, https://github.com/JackalLabs/canine-chain/blob/master/x/lp/types/keys.go#L5
			"oracle",        // https://github.com/JackalLabs/canine-chain/blob/master/x/oracle/types/keys.go#L5
		)

		for key, value := range jackalKeys {
			keys[key] = value
		}
	} else if app == "kichain" {
		kichainKeys := storetypes.NewKVStoreKeys(
			"wasm", // wasm.StoreKey,
		)

		for key, value := range kichainKeys {
			keys[key] = value
		}
	} else if app == "cyber" {
		cyberKeys := storetypes.NewKVStoreKeys(
			"liquidity", // liquiditytypes.StoreKey,
			"bandwidth", // bandwidthtypes.StoreKey,
			"graph",     // graphtypes.StoreKey,
			"rank",      // ranktypes.StoreKey,
			"grid",      // gridtypes.StoreKey,
			"dmn",       // dmntypes.StoreKey,
			"wasm",      // wasm.StoreKey,
		)

		for key, value := range cyberKeys {
			keys[key] = value
		}
	} else if app == "cheqd" {
		cheqdKeys := storetypes.NewKVStoreKeys(
			"cheqd", // cheqdtypes.StoreKey,
		)

		for key, value := range cheqdKeys {
			keys[key] = value
		}
	} else if app == "stargaze" {
		stargazeKeys := storetypes.NewKVStoreKeys(
			"claim", // claimmoduletypes.StoreKey,
			"alloc", // allocmoduletypes.StoreKey,
			"wasm",  // wasm.StoreKey,
		)

		for key, value := range stargazeKeys {
			keys[key] = value
		}
	} else if app == "bandchain" {
		bandchainKeys := storetypes.NewKVStoreKeys(
			"oracle", // oracletypes.StoreKey,
		)

		for key, value := range bandchainKeys {
			keys[key] = value
		}
	} else if app == "chihuahua" {
		chihuahuaKeys := storetypes.NewKVStoreKeys(
			"wasm", // wasm.StoreKey,
		)

		for key, value := range chihuahuaKeys {
			keys[key] = value
		}
	} else if app == "bitcanna" {
		bitcannaKeys := storetypes.NewKVStoreKeys(
			"bcna", // bcnamoduletypes.StoreKey,
		)

		for key, value := range bitcannaKeys {
			keys[key] = value
		}
	} else if app == "konstellation" {
		konstellationKeys := storetypes.NewKVStoreKeys(
			"oracle", // racletypes.StoreKey,
			"wasm",   // wasm.StoreKey,
		)

		for key, value := range konstellationKeys {
			keys[key] = value
		}
	} else if app == "omniflixhub" {
		omniflixhubKeys := storetypes.NewKVStoreKeys(
			"alloc",       // alloctypes.StoreKey,
			"onft",        // onfttypes.StoreKey,
			"marketplace", // marketplacetypes.StoreKey,
		)

		for key, value := range omniflixhubKeys {
			keys[key] = value
		}
	} else if app == "vidulum" {
		vidulumKeys := storetypes.NewKVStoreKeys(
			"vidulum", // vidulummoduletypes.StoreKey,
		)

		for key, value := range vidulumKeys {
			keys[key] = value
		}
	} else if app == "beezee" {
		beezeeKeys := storetypes.NewKVStoreKeys(
			"scavenge", //scavengemodule.Storekey,
		)

		for key, value := range beezeeKeys {
			keys[key] = value
		}
	} else if app == "provenance" {
		provenanceKeys := storetypes.NewKVStoreKeys(
			"metadata",  // metadatatypes.StoreKey,
			"marker",    // markertypes.StoreKey,
			"attribute", // attributetypes.StoreKey,
			"name",      // nametypes.StoreKey,
			"msgfees",   // msgfeestypes.StoreKey,
			"wasm",      // wasm.StoreKey,
		)

		for key, value := range provenanceKeys {
			keys[key] = value
		}
	} else if app == "dig" {
		digKeys := storetypes.NewKVStoreKeys(
			"wasm", // wasm.StoreKey,
		)

		for key, value := range digKeys {
			keys[key] = value
		}
	} else if app == "comdex" {
		comdexKeys := storetypes.NewKVStoreKeys(
			"wasm", // wasm.StoreKey,
		)

		for key, value := range comdexKeys {
			keys[key] = value
		}
	} else if app == "bitsong" {
		bitsongKeys := storetypes.NewKVStoreKeys(
			"packetfowardmiddleware", // routertypes.StoreKey,
			"fantoken",               // fantokentypes.StoreKey,
			"merkledrop",             // merkledroptypes.StoreKey,
		)

		for key, value := range bitsongKeys {
			keys[key] = value
		}
	} else if app == "assetmantle" {
		assetmantleKeys := storetypes.NewKVStoreKeys(
			"packetfowardmiddleware", // routerTypes.StoreKey,
			"icahost",                // icaHostTypes.StoreKey,
		)

		for key, value := range assetmantleKeys {
			keys[key] = value
		}
	} else if app == "fetchhub" {
		fetchhubKeys := storetypes.NewKVStoreKeys(
			"wasm", // wasm.StoreKey,
		)

		for key, value := range fetchhubKeys {
			keys[key] = value
		}
	} else if app == "persistent" {
		persistentKeys := storetypes.NewKVStoreKeys(
			"halving", // halving.StoreKey,
		)

		for key, value := range persistentKeys {
			keys[key] = value
		}
	} else if app == "cryptoorgchain" {
		cryptoorgchainKeys := storetypes.NewKVStoreKeys(
			"chainmain", // chainmaintypes.StoreKey,
			"supply",    // supplytypes.StoreKey,
			"nft",       // nfttypes.StoreKey,
		)

		for key, value := range cryptoorgchainKeys {
			keys[key] = value
		}
	} else if app == "irisnet" {
		irisnetKeys := storetypes.NewKVStoreKeys(
			"guardian", // guardiantypes.StoreKey,
			"token",    // tokentypes.StoreKey,
			"nft",      // nfttypes.StoreKey,
			"htlc",     // htlctypes.StoreKey,
			"record",   // recordtypes.StoreKey,
			"coinswap", // coinswaptypes.StoreKey,
			"service",  // servicetypes.StoreKey,
			"oracle",   // oracletypes.StoreKey,
			"random",   // randomtypes.StoreKey,
			"farm",     // farmtypes.StoreKey,
			"tibc",     // t"ibc",
			"NFT",      // tibcnfttypes.StoreKey,
			"MT",       // tibcmttypes.StoreKey,
			"mt",       // mttypes.StoreKey,
		)

		for key, value := range irisnetKeys {
			keys[key] = value
		}
	} else if app == "axelar" {
		axelarKeys := storetypes.NewKVStoreKeys(
			"vote",       // voteTypes.StoreKey,
			"evm",        // evmTypes.StoreKey,
			"snapshot",   // snapTypes.StoreKey,
			"multisig",   // multisigTypes.StoreKey,
			"tss",        // tssTypes.StoreKey,
			"nexus",      // nexusTypes.StoreKey,
			"axelarnet",  // axelarnetTypes.StoreKey,
			"reward",     // rewardTypes.StoreKey,
			"permission", // permissionTypes.StoreKey,
			"wasm",       // wasm.StoreKey,
		)

		for key, value := range axelarKeys {
			keys[key] = value
		}
	} else if app == "umee" {
		umeeKeys := storetypes.NewKVStoreKeys(
			"gravity", // gravitytypes.StoreKey,
		)

		for key, value := range umeeKeys {
			keys[key] = value
		}
	} else if app == "desmos" {
		// https://github.com/desmos-labs/desmos/blob/master/app/app.go#L255
		desmosKeys := storetypes.NewKVStoreKeys(
			// common modules
			"wasm", // wasm.StoreKey,
			// IBC modules
			"icacontroller", // icacontrollertypes.StoreKey, https://github.com/cosmos/ibc-go/blob/main/modules/apps/27-interchain-accounts/controller/types/keys.go#L5
			"icahost",       // icahosttypes.StoreKey,
			// mainnet since v4.7.0
			"profiles",      // profilestypes.StoreKey,
			"relationships", // relationshipstypes.StoreKey,
			"subspaces",     // subspacestypes.StoreKey,
			"posts",         // poststypes.StoreKey,
			"reports",       // reports.StoreKey,
			"reactions",     // reactions.StoreKey,
			"fees",          // fees.StoreKey,
			// mainnet since v6.0
			"supply",
			"tokenfactory",
		)

		for key, value := range desmosKeys {
			keys[key] = value
		}
	}

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
		
		// Prune in batches
		batchSize := int64(10000)
		deletionRange := actualPruneToVersion - firstVer + 1
		totalBatches := (deletionRange + batchSize - 1) / batchSize
		fmt.Printf("  Pruning in %d batches (deleting %d-%d)...\n", totalBatches, firstVer, actualPruneToVersion)
		
		batchCount := 0
		hadError := false
		
		for currentVer := firstVer; currentVer < actualPruneToVersion; currentVer += batchSize {
			deleteUpTo := currentVer + batchSize - 1
			if deleteUpTo > actualPruneToVersion {
				deleteUpTo = actualPruneToVersion
			}
			
			batchCount++
			if batchCount%5 == 1 || batchCount == int(totalBatches) {
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
