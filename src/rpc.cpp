// rpc.cpp - implementation of the rpc namespace

#include "rpc.hpp"

#include <chrono>
#include <curl/curl.h>
#include <iostream>
#include <sstream>
#include <thread>

namespace rpc {

// cURL write callback - appends received data to a std::string.
static size_t curl_write_cb(void* ptr,
    size_t size,
    size_t nmemb,
    void* userp)
{
    std::string* dst = static_cast<std::string*>(userp);
    dst->append(static_cast<char*>(ptr), size * nmemb);
    return size * nmemb;
}

// Low-level RPC call (member of Client)
std::optional<std::string>
Client::call(std::string_view payload)
{
    CURL* curl = curl_easy_init();
    if (!curl) {
        std::cerr << "[rpc] curl_easy_init() failed\n";
        return std::nullopt;
    }

    std::string response;
    curl_easy_setopt(curl, CURLOPT_URL, url_.c_str());
    curl_easy_setopt(curl, CURLOPT_POST, 1L);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, payload.data());
    curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE,
        static_cast<long>(payload.size()));
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, curl_write_cb);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, 3'600'000L); // 1 h

    struct curl_slist* headers = nullptr;
    headers = curl_slist_append(headers,
        "Content-Type: application/json");
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

    CURLcode rc = curl_easy_perform(curl);
    curl_easy_cleanup(curl);
    curl_slist_free_all(headers);

    if (rc != CURLE_OK) {
        std::cerr << "[rpc] curl error: " << curl_easy_strerror(rc)
                  << "\n";
        return std::nullopt;
    }
    return response;
}

// Build the eth_getLogs payload (member of Client)
std::string
Client::make_get_logs_payload(uint64_t from,
    uint64_t to,
    std::string_view topic)
{
    json req {
        { "jsonrpc", "2.0" },
        { "id", 1 },
        { "method", "eth_getLogs" },
        { "params", json::array({ { { "fromBlock", u64_to_hex(from) }, { "toBlock", u64_to_hex(to) }, { "topics", json::array({ std::string(topic) }) } } }) }
    };
    return req.dump();
}

// High-level fetch + parse (member of Client)
Client::RpcResult
Client::fetch_log_range(uint64_t from,
    uint64_t to,
    std::string_view topic)
{
    RpcResult r;
    r.firstBlock = from;

    const std::string payload = make_get_logs_payload(from, to, topic);
    auto raw_opt = call(payload);
    if (!raw_opt) {
        std::cerr << "[rpc] request failed for range [" << from
                  << ", " << to << "]\n";
        // r.ok stays false
        return r;
    }

    try {
        r.payload = json::parse(*raw_opt);
        r.ok = true;
    } catch (const json::parse_error& e) {
        std::cerr << "[rpc] JSON parse error for range [" << from
                  << ", " << to << "]: " << e.what() << '\n';
    }
    return r;
}

// Helper that parses a hex string like "0x1a3f" into a uint64_t.
static std::optional<uint64_t> hex_to_u64(const std::string& hex)
{
    std::uint64_t v = 0;
    std::istringstream iss(hex);
    iss >> std::hex >> v;
    if (iss.fail())
        return std::nullopt;
    return v;
}

// eth_blockNumber RPC call - returns the tip of the chain.
std::optional<uint64_t>
Client::get_latest_block()
{
    // Build a minimal JSON-RPC request.
    json req {
        { "jsonrpc", "2.0" },
        { "id", 1 },
        { "method", "eth_blockNumber" },
        { "params", json::array() }
    };
    const std::string payload = req.dump();

    // uses the stored URL
    auto raw_opt = call(payload);
    if (!raw_opt)
        return std::nullopt;

    try {
        json resp = json::parse(*raw_opt);
        if (!resp.contains("result"))
            return std::nullopt;

        // The result is a hex string, e.g. "0x1a3f"
        return hex_to_u64(resp["result"].get<std::string>());
    } catch (const json::exception& e) {
        std::cerr << "[rpc] get_latest_block JSON error: " << e.what() << '\n';
        return std::nullopt;
    }
}

} // namespace rpc
