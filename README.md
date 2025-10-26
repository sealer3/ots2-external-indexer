# ots2-external-indexer
Provides external indexers of token transfers and holdings for EVM blockchains in support of the `ots2` namespace for [Otterscan](https://github.com/otterscan/otterscan).

**Note: This project is currently an early preview. It's experimental and unoptimized. Everything is likely to change.**

Currently implemented:
- `ots2_getERC20Holdings`
- `ots2_getERC20TransferList`
- `ots2_getERC20TransferCount`
- `ots2_getERC721TransferList`
- `ots2_getERC721TransferCount`

### Requirements

1. On Debian/Ubuntu systems, ensure the following packages are installed:
```
sudo apt install -y cmake build-essential pkg-config nlohmann-json3-dev libcurl4-openssl-dev libcpp-httplib-dev libcli11-dev libleveldb-dev
```

2. Ensure you are running an execution node for your target EVM blockchain either on your local machine or on a local network.
   - Syncing a remote node might take an extremely long time, and the initial sync uses lots of node resources.
   - Any execution node which supports `eth_getLogs` and `eth_getBlockByNumber` is compatible.

### Building

```
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j$(nproc)
```

### Running

Currently the event syncer and RPC server are separate programs. They will be combined.

First, sync with the execution node, saving the indexer database to `./eventsdb`:

```
./build/bin/event_syncer ./eventsdb --rpc-url http://localhost:8545
```

Interrupt the event syncer when finished. Then, serve (defaults to listening on `http://localhost:17444`):

```
./build/bin/rpc_server ./eventsdb --rpc-url http://localhost:8545
```

### Contributing

Run `clang-format -i src/*` for code formatting.
