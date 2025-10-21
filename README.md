# ots-external-indexers
Provides external indexers of token transfers and holdings for EVM blockchains in support of Otterscan's `ots2` namespace.

**Note: This project is currently an early preview. It's experimental and unoptimized. Everything is likely to change.**

Currently implemented:
- `ots2_getERC20Holdings`
- `ots2_getERC20TransferList`
- `ots2_getERC20TransferCount`
- `ots2_getERC721TransferList`
- `ots2_getERC721TransferCount`

### Requirements

On Debian/Ubuntu systems, ensure the following packages are installed:
```
sudo apt install -y nlohmann-json3-dev libcurl4-openssl-dev libcpp-httplib-dev
```

### Building

```
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j$(nproc)
```

### Running

Currently the event syncer and RPC server are separate programs. They will be combined.

First, sync:

```
./build/bin/event_syncer
```

Then, serve:

```
./build/bin/rpc_server
```

### Contributing

Run `clang-format -i src/*` for code formatting.
