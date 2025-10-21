// rpc.hpp - JSON-RPC / cURL helpers

#pragma once

#include <cstdint>
#include <future>
#include <nlohmann/json.hpp>
#include <optional>
#include <string>
#include <string_view>

namespace rpc {
using json = nlohmann::json;

// Convert a uint64_t block number to the hex string expected by the RPC (e.g. 0x1a3f).
// Declared inline so the implementation can stay in the header; it's trivial.
inline std::string u64_to_hex(uint64_t v)
{
    std::ostringstream oss;
    oss << "0x" << std::hex << v;
    return oss.str();
}

class Client {
public:
    // Construct a client for the given endpoint URL.
    explicit Client(std::string url = "http://localhost:8545")
        : url_(std::move(url))
    {
    }

    // Result of a single eth_getLogs request.
    struct RpcResult {
        // the `fromBlock` used for the request
        uint64_t firstBlock { 0 };
        // parsed JSON (empty on error)
        json payload;
        bool ok { false };
    };

    // Low-level wrapper around libcurl; returns raw response body.
    [[nodiscard]] std::optional<std::string>
    call(std::string_view payload);

    // Build the JSON-RPC payload for eth_getLogs.
    [[nodiscard]] std::string
    make_get_logs_payload(uint64_t from, uint64_t to, std::string_view topic);

    // High-level helper that does the whole request and parses JSON.
    [[nodiscard]] RpcResult
    fetch_log_range(uint64_t from, uint64_t to, std::string_view topic);

    // Query the node for the current block number (eth_blockNumber).
    [[nodiscard]] std::optional<uint64_t>
    get_latest_block();

private:
    // endpoint URL
    std::string url_;
};

} // namespace rpc
