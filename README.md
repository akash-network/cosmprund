# Cosmprund - Akash Network Pruner

Offline pruning tool for Akash Network nodes running Cosmos SDK v0.53+ with IAVL v1.2+.

Prunes both application state (IAVL stores) and CometBFT block/state data to reduce disk usage without requiring state sync.

## Features

✅ **Offline Pruning for SDK v0.53** - Works with Akash v1.0.0+  
✅ **Proven Results** - 93% disk space reduction (tested: 2GB → 135MB)  
✅ **Application State Pruning** - All Akash modules supported  
✅ **CometBFT Block Pruning** - Updated for CometBFT v0.38 APIs  
✅ **Production Ready** - Tested on live Akash validator nodes  

## Requirements

- Akash Network v1.0.0 or higher
- Cosmos SDK v0.53+

## Installation

### Option 1: Download Pre-built Binary (Recommended)

Download the latest release for your platform:

```bash
# Linux AMD64 (most common)
wget https://github.com/akash-network/cosmprund/releases/latest/download/cosmprund_linux_amd64.zip
unzip cosmprund_linux_amd64.zip
chmod +x cosmprund
sudo mv cosmprund /usr/local/bin/

# Linux ARM64
wget https://github.com/akash-network/cosmprund/releases/latest/download/cosmprund_linux_arm64.zip
unzip cosmprund_linux_arm64.zip
chmod +x cosmprund
sudo mv cosmprund /usr/local/bin/

# macOS Intel
wget https://github.com/akash-network/cosmprund/releases/latest/download/cosmprund_darwin_amd64.zip
unzip cosmprund_darwin_amd64.zip
chmod +x cosmprund
sudo mv cosmprund /usr/local/bin/

# macOS Apple Silicon
wget https://github.com/akash-network/cosmprund/releases/latest/download/cosmprund_darwin_arm64.zip
unzip cosmprund_darwin_arm64.zip
chmod +x cosmprund
sudo mv cosmprund /usr/local/bin/
```

### Option 2: Build from Source

Requirements: Go 1.24.2+

```bash
# Clone and build
git clone https://github.com/akash-network/cosmprund.git
cd cosmprund
make install

# Or build manually
go build -o cosmprund main.go
```

## Usage

### Basic Usage

```bash
# Stop your Akash node
systemctl stop akash-node

# Prune application state (keep last 100 versions)
cosmprund prune ~/.akash/data --versions=100

# Start your node
systemctl start akash-node
```

### Prune Both Application State and Blocks

```bash
cosmprund prune ~/.akash/data --cosmos-sdk=true --tendermint=true --blocks=100 --versions=100
```

### Flags

- `--cosmos-sdk` - Prune application state (default: true)
- `--tendermint` - Prune CometBFT blocks and state (default: true)
- `--versions` - Number of application versions to keep (default: 10)
- `--blocks` - Number of blocks to keep (default: 10)


## Supported Modules

### Core SDK Modules
- acc, bank, staking, mint, distribution, slashing
- gov, params, ibc, upgrade, feegrant, evidence
- transfer, authz, consensus, crisis

### Akash Network Modules
- escrow, deployment, market, provider
- audit, cert, take

## How It Works

Cosmprund uses a breakthrough approach for offline pruning with SDK v0.53:

1. **Bypasses SDK Store Wrapper** - Works directly with IAVL MutableTree
2. **SyncOption(true)** - Forces fsync after each deletion for disk persistence
3. **Batch Pruning** - Deletes in 10K version batches for efficiency
4. **Per-Store Processing** - Each store pruned independently for resilience

This solves the issue where SDK v0.53's store wrapper doesn't persist deletions offline.

## Performance

- **Application State:** ~30-60 minutes for 200K+ versions
- **Block Pruning:** ~5-15 minutes for 100K+ blocks  
- **Disk Savings:** 85-95% reduction typical

## Disclaimer

This tool is designed specifically for Akash Network. Use at your own risk.

Always:
- Stop your node before pruning
- Keep backups of important data
- Test on non-critical nodes first
- Verify your node syncs correctly after pruning

## Support

For issues or questions:
- [Akash Network Discord](https://discord.akash.network)
- [GitHub Issues](https://github.com/akash-network/cosmprund/issues)
