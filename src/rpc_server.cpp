// rpc_server.cpp - JSON-RPC gateway (CORS-enabled)
// Listens on 127.0.0.1:17444 and implements custom methods
// All other requests are proxied unchanged to a real execution node.

#include "event_db.hpp"

#define CPPHTTPLIB_OPENSSL_SUPPORT
#include <CLI/CLI.hpp>
#include <algorithm>
#include <cctype>
#include <cstdint>
#include <filesystem>
#include <httplib.h>
#include <iostream>
#include <nlohmann/json.hpp>
#include <string>
#include <unordered_map>
#include <vector>

using json = nlohmann::json;
using namespace eventdb;

// Global configuration filled by CLI11 at startup
static std::string g_rpc_url = "http://localhost:8545";
static uint16_t g_listen_port = 17444;

// Helper: convert a 32-byte blob (transaction hash) to a 0x-prefixed
// lowercase hex string.
static std::string blob_to_hex(const EventDB::Blob& b)
{
    static const char* hex = "0123456789abcdef";
    std::string out = "0x";
    out.reserve(2 + b.size() * 2);
    for (unsigned char c : b) {
        out.push_back(hex[(c >> 4) & 0xF]);
        out.push_back(hex[c & 0xF]);
    }
    return out;
}

// Simple wrapper that forwards a JSON-RPC request to the Ethereum
// node (URL taken from the command line) and returns the raw response body.
static std::string forward_to_eth_node(const std::string& payload)
{
    // httplib can be constructed directly from a full URL (scheme+host+port)
    httplib::Client eth_client(g_rpc_url);
    eth_client.set_read_timeout(30, 0); // seconds

    auto res = eth_client.Post("/", payload, "application/json");
    if (!res) {
        throw std::runtime_error("Failed to reach Ethereum JSON-RPC node at " + g_rpc_url);
    }
    if (res->status != 200) {
        throw std::runtime_error("Ethereum node returned HTTP " + std::to_string(res->status));
    }
    return res->body;
}

// Retrieve the transaction (eth_getTransactionByHash) and receipt
// (eth_getTransactionReceipt) for a given hash. Returns a pair
// <tx_json, receipt_json>.
static std::pair<json, json> fetch_tx_and_receipt(const std::string& tx_hash)
{
    // 1) eth_getTransactionByHash
    json req_tx = {
        { "jsonrpc", "2.0" },
        { "method", "eth_getTransactionByHash" },
        { "params", { tx_hash } },
        { "id", 1 }
    };
    std::string body_tx = forward_to_eth_node(req_tx.dump());
    json resp_tx = json::parse(body_tx);
    if (resp_tx.contains("error"))
        throw std::runtime_error("eth_getTransactionByHash error: " + resp_tx["error"].dump());

    // 2) eth_getTransactionReceipt
    json req_rcpt = {
        { "jsonrpc", "2.0" },
        { "method", "eth_getTransactionReceipt" },
        { "params", { tx_hash } },
        { "id", 2 }
    };
    std::string body_rcpt = forward_to_eth_node(req_rcpt.dump());
    json resp_rcpt = json::parse(body_rcpt);
    if (resp_rcpt.contains("error"))
        throw std::runtime_error("eth_getTransactionReceipt error: " + resp_rcpt["error"].dump());

    return { resp_tx["result"], resp_rcpt["result"] };
}

// Obtain the block timestamp (seconds since epoch) for a given block number.
// The block number must be a hex string like "0xb26".
static uint64_t fetch_block_timestamp(const std::string& block_number_hex)
{
    json req = {
        { "jsonrpc", "2.0" },
        { "method", "eth_getBlockByNumber" },
        { "params", { block_number_hex, false } }, // false => no tx objects needed
        { "id", 1 }
    };
    std::string body = forward_to_eth_node(req.dump());
    json resp = json::parse(body);
    if (resp.contains("error"))
        throw std::runtime_error("eth_getBlockByNumber error: " + resp["error"].dump());

    json block = resp["result"];
    // block["timestamp"] is a hex string, e.g. "0x651f1a70"
    std::string ts_hex = block["timestamp"];
    uint64_t ts = std::stoull(ts_hex.substr(2), nullptr, 16);
    return ts;
}

// Generic implementation of the TransferCount method.
// The only difference between ERC-20 and ERC-721 is the prefix used
// in the underlying EventDB list ("E" for ERC-20, "N" for ERC-721).
static json handle_getTransferCount_generic(EventDB& db,
    const json& params,
    const std::string& list_prefix)
{
    if (!params.is_array() || params.size() != 1 || !params[0].is_string())
        throw std::invalid_argument("Invalid parameters for getTransferCount");

    std::string address = params[0].get<std::string>();
    // Normalise - lower-case
    std::transform(address.begin(), address.end(), address.begin(),
        [](unsigned char c) { return std::tolower(c); });

    uint64_t count = db.getListCount(list_prefix, address);
    return static_cast<int64_t>(count);
}

// Generic implementation of the TransferList method.
// Parameters: [address, offset, pageSize]
// Result: object with two members:
//   - blocksSummary: map blockNumberHex -> {blockNumber, timestamp}
//   - results: list of {hash, transaction, receipt}
// The list is sorted by block timestamp (ascending).
static json handle_getTransferList_generic(EventDB& db,
    const json& params,
    const std::string& list_prefix)
{
    if (!params.is_array() || params.size() != 3 || !params[0].is_string() || !params[1].is_number_integer() || !params[2].is_number_integer())
        throw std::invalid_argument("Invalid parameters for getTransferList");

    std::string address = params[0].get<std::string>();
    int64_t offset = params[1].get<int64_t>();
    int64_t pageSize = params[2].get<int64_t>();

    // Normalise address
    std::transform(address.begin(), address.end(), address.begin(),
        [](unsigned char c) { return std::tolower(c); });

    uint64_t total = db.getListCount(list_prefix, address);

    if (offset < 0)
        offset = 0; // safety
    if (pageSize < 0)
        pageSize = 0;

    int64_t end = static_cast<int64_t>(total) - offset;
    if (end < 0)
        end = 0;

    int64_t start = std::max<int64_t>(0, end - pageSize);
    int64_t len = std::min<int64_t>(end - start, pageSize); // actual number of items to fetch

    if (static_cast<uint64_t>(offset) > total)
        offset = static_cast<int64_t>(total);

    // Nothing to return → empty result object
    if (len == 0) {
        json empty = json {
            { "blocksSummary", json::object() },
            { "results", json::array() }
        };
        return empty;
    }

    // 1) Pull the wanted transaction hashes from EventDB
    std::vector<EventDB::Blob> blobs = db.getRecentFromList(list_prefix, address,
        static_cast<size_t>(len),
        static_cast<size_t>(start));

    // 2) For each hash ask the Ethereum node for transaction & receipt.
    //    Also collect the distinct block numbers we encounter.
    struct Item {
        std::string hash; // hex string
        json tx; // eth_getTransaction result
        json receipt; // eth_getTransactionReceipt result
        std::string blockNumberHex; // e.g. "0xb26"
        uint64_t blockTimestamp; // seconds since epoch
    };
    std::vector<Item> items;
    items.reserve(blobs.size());

    std::unordered_map<std::string, uint64_t> block_ts_cache; // blockHex -> timestamp

    for (const auto& blob : blobs) {
        std::string hash = blob_to_hex(blob);
        auto [tx, receipt] = fetch_tx_and_receipt(hash);

        if (!tx.contains("blockNumber") || tx["blockNumber"].is_null()) {
            std::cout << "Transaction without blockNumber (skipped): " << hash << std::endl;
            continue;
        }

        std::string blk_hex = tx["blockNumber"].get<std::string>();

        uint64_t ts = 0;
        auto it = block_ts_cache.find(blk_hex);
        if (it == block_ts_cache.end()) {
            ts = fetch_block_timestamp(blk_hex);
            block_ts_cache.emplace(blk_hex, ts);
        } else {
            ts = it->second;
        }

        items.push_back({ hash, std::move(tx), std::move(receipt),
            blk_hex, ts });
    }

    // 3) Sort the items into reverse order
    std::reverse(items.begin(), items.end());

    // 4) Build the blocksSummary object
    json blocksSummary = json::object();
    for (const auto& kv : block_ts_cache) {
        blocksSummary[kv.first] = {
            { "blockNumber", kv.first },
            { "timestamp", kv.second }
        };
    }

    // 5) Build the final result array
    json results = json::array();
    for (const auto& it : items) {
        results.push_back({ { "hash", it.hash },
            { "transaction", it.tx },
            { "receipt", it.receipt } });
    }

    json result = {
        { "blocksSummary", std::move(blocksSummary) },
        { "results", std::move(results) }
    };
    return result;
}

// Thin wrappers that bind the generic implementation to the concrete
// method names required by the JSON-RPC spec.
static json handle_getERC20TransferCount(EventDB& db, const json& params)
{
    return handle_getTransferCount_generic(db, params, "E");
}
static json handle_getERC721TransferCount(EventDB& db, const json& params)
{
    return handle_getTransferCount_generic(db, params, "N");
}

static json handle_getERC20TransferList(EventDB& db, const json& params)
{
    return handle_getTransferList_generic(db, params, "E");
}
static json handle_getERC721TransferList(EventDB& db, const json& params)
{
    return handle_getTransferList_generic(db, params, "N");
}

// Custom method: ots2_getERC20Holdings
// Params: [address]
// Result: array of objects { "address": "<token-address>" }
static json handle_getHoldings(EventDB& db, const json& params)
{
    if (!params.is_array() || params.size() != 1 || !params[0].is_string())
        throw std::invalid_argument("Invalid parameters for ots2_getERC20Holdings");

    std::string address = params[0].get<std::string>();
    std::transform(address.begin(), address.end(), address.begin(),
        [](unsigned char c) { return std::tolower(c); });

    std::vector<std::string> holdings = db.listHoldings(address);

    json result = json::array();
    for (const auto& tok : holdings) {
        result.push_back({ { "address", "0x" + tok } });
    }
    return result;
}

// CORS helper, adds the three most common headers.
static void set_cors_headers(httplib::Response& res)
{
    // Allow any origin
    res.set_header("Access-Control-Allow-Origin", "*");
    res.set_header("Access-Control-Allow-Methods", "POST, OPTIONS");
    // Which request-headers the client is allowed to send.
    res.set_header("Access-Control-Allow-Headers", "Content-Type, Authorization");
}

// Main: start the HTTP server, dispatch the four custom methods,
// proxy everything else and add CORS support.
int main(int argc, char* argv[])
{
    // Command line parsing (CLI11)
    CLI::App app {
        "ERC-20 / ERC-721 RPC gateway (CORS-enabled)\n"
        "Listens on 127.0.0.1:<gateway-port> (default 17444) and proxies\n"
        "all JSON-RPC calls to a real execution node whose URL is supplied\n"
        "with --rpc-url."
    };

    // Positional argument: the DB folder (required)
    std::filesystem::path db_path;
    app.add_option("db_path", db_path,
           "Path to the LevelDB folder")
        ->required()
        ->check(CLI::ExistingDirectory);

    app.add_option("-u,--rpc-url", g_rpc_url,
           "Execution node JSON-RPC URL")
        ->default_val(g_rpc_url);

    app.add_option("-p,--port", g_listen_port,
           "port on which the gateway will listen (default 17444)")
        ->default_val(g_listen_port)
        ->check(CLI::PositiveNumber);

    app.set_help_all_flag("--help-all", "Show all help");

    try {
        app.parse(argc, argv);
    } catch (const CLI::ParseError& e) {
        // CLI11 already printed a helpful message
        return app.exit(e);
    }

    EventDB event_db(db_path);

    // httplib server on 127.0.0.1:<g_listen_port>
    httplib::Server svr;

    // OPTIONS: respond to CORS pre-flight requests.
    svr.Options("/", [&](const httplib::Request&, httplib::Response& res) {
        set_cors_headers(res);
        // No body needed; 200 OK is enough.
        res.set_content("", "text/plain");
    });

    // POST: regular JSON-RPC handling (custom methods + proxy).
    svr.Post("/", [&](const httplib::Request& req, httplib::Response& res) {
        // CORS headers on every response
        set_cors_headers(res);

        // Parse the incoming body, either a single object or a batch array
        json root;
        try {
            root = json::parse(req.body);
        } catch (const std::exception&) {
            json err = {
                { "jsonrpc", "2.0" },
                { "id", nullptr },
                { "error", { { "code", -32700 }, { "message", "Parse error" } } }
            };
            res.set_content(err.dump(), "application/json");
            return;
        }

        // Helper: produce a JSON-RPC error object for a given id / exception
        auto make_error = [&](const json& id, int code, const std::string& msg) {
            return json {
                { "jsonrpc", "2.0" },
                { "id", id },
                { "error", { { "code", code }, { "message", msg } } }
            };
        };

        // If this is **not** a batch, keep the original simple flow.
        if (!root.is_array()) {
            json request = root;
            json response = {
                { "jsonrpc", "2.0" },
                { "id", request.value("id", json(nullptr)) }
            };

            try {
                std::string method = request.value("method", "");
                json params = request.value("params", json::array());

                if (method == "ots2_getERC20TransferCount") {
                    response["result"] = handle_getERC20TransferCount(event_db, params);
                } else if (method == "ots2_getERC20TransferList") {
                    response["result"] = handle_getERC20TransferList(event_db, params);
                } else if (method == "ots2_getERC721TransferCount") {
                    response["result"] = handle_getERC721TransferCount(event_db, params);
                } else if (method == "ots2_getERC721TransferList") {
                    response["result"] = handle_getERC721TransferList(event_db, params);
                } else if (method == "ots2_getERC20Holdings") {
                    response["result"] = handle_getHoldings(event_db, params);
                } else {
                    // Proxy everything else unchanged
                    std::string forwarded_body = forward_to_eth_node(req.body);
                    res.set_content(forwarded_body, "application/json");
                    return;
                }

                res.set_content(response.dump(), "application/json");
                return;
            } catch (const std::invalid_argument& e) {
                response = make_error(response["id"], -32602, e.what());
            } catch (const std::runtime_error& e) {
                response = make_error(response["id"], -32000, e.what());
            } catch (const std::exception& e) {
                response = make_error(response["id"], -32603,
                    std::string("Internal error: ") + e.what());
            }

            res.set_content(response.dump(), "application/json");
            return;
        }

        // *** BATCH MODE ***
        std::vector<json> custom_responses(root.size(), nullptr);
        std::vector<json> proxy_requests; // will be sent as a batch
        std::unordered_map<json, size_t> id_to_index; // id -> original position

        for (size_t i = 0; i < root.size(); ++i) {
            const json& req_obj = root[i];
            if (!req_obj.is_object()) {
                custom_responses[i] = make_error(nullptr, -32600,
                    "Invalid Request object");
                continue;
            }

            json id = req_obj.value("id", json(nullptr));
            std::string method = req_obj.value("method", "");
            json params = req_obj.value("params", json::array());

            // Custom methods handled locally
            if (method == "ots2_getERC20TransferCount") {
                try {
                    json result = handle_getERC20TransferCount(event_db, params);
                    custom_responses[i] = json {
                        { "jsonrpc", "2.0" },
                        { "id", id },
                        { "result", std::move(result) },
                        { "custom", "yes" },
                    };
                } catch (const std::exception& e) {
                    custom_responses[i] = make_error(id, -32602, e.what());
                }
                continue;
            }
            if (method == "ots2_getERC20TransferList") {
                try {
                    json result = handle_getERC20TransferList(event_db, params);
                    custom_responses[i] = json {
                        { "jsonrpc", "2.0" },
                        { "id", id },
                        { "result", std::move(result) },
                        { "custom", "yes" },
                    };
                } catch (const std::exception& e) {
                    custom_responses[i] = make_error(id, -32602, e.what());
                }
                continue;
            }
            if (method == "ots2_getERC721TransferCount") {
                try {
                    json result = handle_getERC721TransferCount(event_db, params);
                    custom_responses[i] = json {
                        { "jsonrpc", "2.0" },
                        { "id", id },
                        { "result", std::move(result) },
                        { "custom", "yes" },
                    };
                } catch (const std::exception& e) {
                    custom_responses[i] = make_error(id, -32602, e.what());
                }
                continue;
            }
            if (method == "ots2_getERC721TransferList") {
                try {
                    json result = handle_getERC721TransferList(event_db, params);
                    custom_responses[i] = json {
                        { "jsonrpc", "2.0" },
                        { "id", id },
                        { "result", std::move(result) },
                        { "custom", "yes" },
                    };
                } catch (const std::exception& e) {
                    custom_responses[i] = make_error(id, -32602, e.what());
                }
                continue;
            }
            if (method == "ots2_getERC20Holdings") {
                try {
                    json result = handle_getHoldings(event_db, params);
                    custom_responses[i] = json {
                        { "jsonrpc", "2.0" },
                        { "id", id },
                        { "result", std::move(result) },
                        { "custom", "yes" },
                    };
                } catch (const std::exception& e) {
                    custom_responses[i] = make_error(id, -32602, e.what());
                }
                continue;
            }

            // Collect anything else for proxying
            proxy_requests.push_back(req_obj);
            if (!id.is_null())
                id_to_index[id] = i;
            else
                // fallback key for id-less requests
                id_to_index[json(i)] = i;
        }

        // Forward non-custom requests as a single batch
        std::unordered_map<json, json> proxy_responses_by_id;
        if (!proxy_requests.empty()) {
            std::string batch_body = json(proxy_requests).dump();
            std::string forwarded_body = forward_to_eth_node(batch_body);
            json batch_resp = json::parse(forwarded_body);

            if (!batch_resp.is_array())
                throw std::runtime_error("Ethereum node returned non-array batch response");

            for (const auto& resp_obj : batch_resp) {
                json resp_id = resp_obj.value("id", json(nullptr));
                json key = resp_id.is_null() ? json(nullptr) : resp_id;
                proxy_responses_by_id[key] = resp_obj;
            }
        }

        // Assemble final batch response preserving the original order
        json final_response = json::array();

        for (size_t i = 0; i < root.size(); ++i) {
            if (!custom_responses[i].is_null()) {
                final_response.push_back(custom_responses[i]);
                continue;
            }

            const json& orig_req = root[i];
            json id = orig_req.value("id", json(nullptr));
            json key = id.is_null() ? json(nullptr) : id;

            auto it = proxy_responses_by_id.find(key);
            if (it != proxy_responses_by_id.end()) {
                final_response.push_back(it->second);
            } else {
                final_response.push_back(make_error(id, -32603,
                    "Missing response from proxy"));
            }
        }

        res.set_content(final_response.dump(), "application/json");
    });

    std::cout << "Otterscan2 external indexer RPC gateway\n";
    std::cout << "  Listening on 127.0.0.1:" << g_listen_port << "\n";
    std::cout << "  Execution node URL: " << g_rpc_url << "\n";
    std::cout << "  Custom methods:\n";
    std::cout << "        ots2_getERC20TransferCount, ots2_getERC20TransferList\n";
    std::cout << "        ots2_getERC721TransferCount, ots2_getERC721TransferList\n";
    std::cout << "        ots2_getERC20Holdings\n";
    std::cout << "  All other JSON-RPC calls are proxied to the execution node.\n";

    if (!svr.listen("127.0.0.1", g_listen_port)) {
        std::cerr << "Failed to bind to port " << g_listen_port
                  << " - is something else already listening?\n";
        return EXIT_FAILURE;
    }
    return EXIT_SUCCESS; // never reached
}
