// indexer.cpp - ots2 external indexer
// Creates indexes for ERC-20 and ERC-721 transfers
// Listens on 127.0.0.1:17444 and implements custom methods
// All other requests are proxied unchanged to a real execution node.

#include "event_db.hpp"
#include "rpc.hpp"

#define CPPHTTPLIB_OPENSSL_SUPPORT
#include <CLI/CLI.hpp>
#include <algorithm>
#include <cctype>
#include <chrono>
#include <csignal>
#include <filesystem>
#include <future>
#include <httplib.h>
#include <iostream>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

using json = nlohmann::json;
using namespace eventdb;

static std::atomic<bool> g_terminate { false };
static std::atomic<httplib::Server*> g_server_ptr { nullptr };

static void signal_handler(int /*signum*/)
{
    g_terminate.store(true);
    // Stop the HTTP server (if it is already running)
    if (auto* srv = g_server_ptr.load(); srv) {
        srv->stop();
    }
}

static std::string strip_0x(const std::string& s)
{
    return (s.size() >= 2 && s[0] == '0' && (s[1] == 'x' || s[1] == 'X')) ? s.substr(2) : s;
}

// Decode a 0x-prefixed hex string into a 32-byte Blob.
static std::optional<EventDB::Blob> hex_to_blob(const std::string& hex_str)
{
    std::string h = strip_0x(hex_str);
    if (h.size() != 64)
        return std::nullopt;
    EventDB::Blob out {};
    for (size_t i = 0; i < 32; ++i) {
        unsigned int byte {};
        std::sscanf(h.c_str() + i * 2, "%2x", &byte);
        out[i] = static_cast<char>(byte);
    }
    return out;
}

// Extract the last 20 bytes (40 hex chars) of a 32-byte topic -> address
static std::string topic_to_address(const std::string& topic_hex)
{
    std::string h = strip_0x(topic_hex);
    if (h.size() != 64)
        return "";
    // keep the low-order 40 chars (20 bytes)
    return "0x" + h.substr(24);
}

namespace std {
template <class A, class B>
struct hash<pair<A, B>> {
    size_t operator()(const pair<A, B>& p) const noexcept
    {
        // rotate left the first hash by one bit and xor the second
        return std::rotl(hash<A> {}(p.first), 1) ^ hash<B> {}(p.second);
    }
};
}

// Persistent "last synced block" handling
static const std::string kMetaFileName = "last_block.txt";

static uint64_t read_last_block_file(const std::filesystem::path& db_dir)
{
    std::ifstream in(db_dir / kMetaFileName);
    if (!in)
        return 0;
    uint64_t blk {};
    in >> blk;
    if (in.fail()) {
        std::cerr << "Warning: could not parse '" << kMetaFileName
                  << "'. Starting from block 0.\n";
        return 0;
    }
    return blk;
}

static void write_last_block_file(const std::filesystem::path& db_dir,
    uint64_t blk)
{
    std::ofstream out(db_dir / kMetaFileName, std::ios::trunc);
    if (!out) {
        std::cerr << "Error: cannot write last block to '" << kMetaFileName << "'\n";
        return;
    }
    out << blk << '\n';
}

// Forwarding helpers for the RPC-gateway
static std::string g_rpc_url = "http://localhost:8545";

static std::string forward_to_eth_node(const std::string& payload)
{
    httplib::Client eth_client(g_rpc_url);
    eth_client.set_read_timeout(30, 0);
    auto res = eth_client.Post("/", payload, "application/json");
    if (!res)
        throw std::runtime_error("Failed to reach Ethereum JSON-RPC node at " + g_rpc_url);
    if (res->status != 200)
        throw std::runtime_error("Ethereum node returned HTTP " + std::to_string(res->status));
    return res->body;
}

// Sync loop
static void run_sync_loop(EventDB& db,
    const std::filesystem::path& db_path,
    const std::string& rpc_url,
    size_t workers,
    uint64_t batch_range,
    int retry_delay_sec)
{
    rpc::Client rpc_client(rpc_url);

    // Resume
    uint64_t last_saved = read_last_block_file(db_path);
    uint64_t next_block = (last_saved == 0) ? 0 : last_saved + 1;
    std::cout << "[sync] Resuming from block " << next_block
              << " (last saved = " << last_saved << ")\n";

    const uint64_t kBatchRange = batch_range;
    const size_t kWorkers = workers;
    const uint64_t kMaxBatchSize = static_cast<uint64_t>(kWorkers) * kBatchRange;
    const std::string transfer_topic = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";

    while (!g_terminate.load()) {
        // Get the current tip
        std::optional<uint64_t> tip_opt = rpc_client.get_latest_block();
        if (!tip_opt) {
            std::cerr << "[sync] failed to get latest block, retrying in "
                      << retry_delay_sec << " s …\n";
            std::this_thread::sleep_for(std::chrono::seconds(retry_delay_sec));
            continue;
        }
        uint64_t tip = *tip_opt;

        if (next_block > tip) {
            // Nothing new - sleep a bit and re-query tip
            std::this_thread::sleep_for(std::chrono::seconds(5));
            continue;
        }

        uint64_t batch_start = next_block;
        uint64_t batch_end = std::min(batch_start + kMaxBatchSize - 1, tip);

        // Fetch the whole batch (parallel workers)
        bool batch_ok = false;
        std::vector<rpc::Client::RpcResult> results;

        while (!batch_ok && !g_terminate.load()) {
            std::vector<std::future<rpc::Client::RpcResult>> futures;
            futures.reserve(kWorkers);
            for (size_t i = 0; i < kWorkers; ++i) {
                uint64_t from = batch_start + i * kBatchRange;
                uint64_t to = std::min(from + kBatchRange - 1, batch_end);
                if (from <= to) {
                    futures.emplace_back(std::async(std::launch::async,
                        &rpc::Client::fetch_log_range,
                        &rpc_client, from, to, transfer_topic));
                }
            }

            results.clear();
            results.reserve(futures.size());
            for (auto& f : futures)
                results.emplace_back(f.get());

            std::sort(results.begin(), results.end(),
                [](const rpc::Client::RpcResult& a,
                    const rpc::Client::RpcResult& b) {
                    return a.firstBlock < b.firstBlock;
                });

            batch_ok = std::all_of(results.begin(), results.end(),
                [](const rpc::Client::RpcResult& r) { return r.ok; });

            if (!batch_ok) {
                std::cerr << "[sync] batch [" << batch_start << '-' << batch_end
                          << "] had failures - retrying in "
                          << retry_delay_sec << " s …\n";
                std::this_thread::sleep_for(std::chrono::seconds(retry_delay_sec));
            }
        }

        if (g_terminate.load()) {
            // interrupted while retrying
            break;
        }

        // Process logs
        uint64_t events_appended = 0;
        for (const auto& r : results) {
            if (!r.payload.contains("result") || !r.payload["result"].is_array())
                continue;

            // Per-batch dedup structures (reset for each RPC result)
            std::unordered_map<std::string, std::string> lastTxForAddr20;
            std::unordered_map<std::string, std::string> lastTxForAddr721;
            std::unordered_set<std::pair<std::string, std::string>> addressRecordedTokens;

            for (const auto& log : r.payload["result"]) {
                if (!log.contains("topics") || !log["topics"].is_array())
                    continue;
                const auto& topics = log["topics"];
                if (topics.size() != 3 && topics.size() != 4) {
                    // not ERC-20/ERC-721
                    continue;
                }

                bool is_erc20 = (topics.size() == 3);
                bool is_erc721 = (topics.size() == 4);
                const std::string list_prefix = is_erc721 ? "N" : "E";
                auto& lastTxForAddr = is_erc721 ? lastTxForAddr721 : lastTxForAddr20;

                std::string from_addr = topic_to_address(topics[1].get<std::string>());
                std::string to_addr = topic_to_address(topics[2].get<std::string>());
                if (from_addr.empty() || to_addr.empty())
                    continue;

                if (!log.contains("transactionHash"))
                    continue;
                const std::string tx_hash_hex = log["transactionHash"].get<std::string>();
                auto blob_opt = hex_to_blob(tx_hash_hex);
                if (!blob_opt)
                    continue;
                const EventDB::Blob& tx_blob = *blob_opt;

                // Append for both parties, avoiding duplicate tx-hashes per address
                for (const std::string& addr : { from_addr, to_addr }) {
                    auto it = lastTxForAddr.find(addr);
                    if (it == lastTxForAddr.end() || it->second != tx_hash_hex) {
                        db.appendToList(list_prefix, addr, tx_blob);
                        ++events_appended;
                        lastTxForAddr[addr] = tx_hash_hex;
                    }

                    // Record ERC-20 holdings
                    if (is_erc20 && log.contains("address")) {
                        std::string token_addr = strip_0x(log["address"].get<std::string>());
                        auto pair = std::make_pair(addr, token_addr);
                        if (addressRecordedTokens.insert(pair).second) {
                            db.addHolding(addr, token_addr);
                        }
                    }
                }
            }
        }

        // Persist new "last block"
        uint64_t new_last = batch_end;
        write_last_block_file(db_path, new_last);
        last_saved = new_last;
        next_block = new_last + 1;

        std::cout << "[sync] Batch [" << batch_start << '-' << batch_end << "] "
                  << "appended " << events_appended << " events. "
                  << "last saved = " << last_saved << '\n';
    }

    std::cout << "[sync] Terminating - final last-saved block = "
              << last_saved << '\n';
}

// RPC-gateway helpers
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

static void set_cors_headers(httplib::Response& res)
{
    res.set_header("Access-Control-Allow-Origin", "*");
    res.set_header("Access-Control-Allow-Methods", "POST, OPTIONS");
    res.set_header("Access-Control-Allow-Headers", "Content-Type, Authorization");
}

// Generic handler for transfer count (ERC-20 or ERC-721)
static json handle_getTransferCount_generic(EventDB& db,
    const json& params,
    const std::string& list_prefix)
{
    if (!params.is_array() || params.size() != 1 || !params[0].is_string())
        throw std::invalid_argument("Invalid parameters for getTransferCount");

    std::string address = params[0].get<std::string>();
    std::transform(address.begin(), address.end(), address.begin(),
        [](unsigned char c) { return std::tolower(c); });

    uint64_t cnt = db.getListCount(list_prefix, address);
    return static_cast<int64_t>(cnt);
}

// Generic handler for transfer list (ERC-20 or ERC-721)
static json handle_getTransferList_generic(EventDB& db,
    const json& params,
    const std::string& list_prefix)
{
    if (!params.is_array() || params.size() != 3 || !params[0].is_string() || !params[1].is_number_integer() || !params[2].is_number_integer())
        throw std::invalid_argument("Invalid parameters for getTransferList");

    std::string address = params[0].get<std::string>();
    int64_t offset = params[1].get<int64_t>();
    int64_t pageSize = params[2].get<int64_t>();

    std::transform(address.begin(), address.end(), address.begin(),
        [](unsigned char c) { return std::tolower(c); });

    uint64_t total = db.getListCount(list_prefix, address);
    if (offset < 0)
        offset = 0;
    if (pageSize < 0)
        pageSize = 0;

    int64_t end = static_cast<int64_t>(total) - offset;
    if (end < 0)
        end = 0;
    int64_t start = std::max<int64_t>(0, end - pageSize);
    int64_t len = std::min<int64_t>(end - start, pageSize);
    if (static_cast<uint64_t>(offset) > total)
        offset = static_cast<int64_t>(total);

    if (len == 0) {
        return json {
            { "blocksSummary", json::object() },
            { "results", json::array() }
        };
    }

    // 1) fetch blobs
    std::vector<EventDB::Blob> blobs = db.getRecentFromList(list_prefix, address,
        static_cast<size_t>(len),
        static_cast<size_t>(start));

    // 2) resolve each hash via the underlying node
    struct Item {
        std::string hash;
        json tx;
        json receipt;
        std::string blockNumberHex;
        uint64_t blockTimestamp;
    };
    std::vector<Item> items;
    items.reserve(blobs.size());

    std::unordered_map<std::string, uint64_t> block_ts_cache;

    for (const auto& blob : blobs) {
        std::string hash = blob_to_hex(blob);
        auto [tx, receipt] = [&] {
            // reuse the forward helper - it already knows g_rpc_url
            json req_tx = {
                { "jsonrpc", "2.0" },
                { "method", "eth_getTransactionByHash" },
                { "params", { hash } },
                { "id", 1 }
            };
            json tx_res = json::parse(forward_to_eth_node(req_tx.dump()));
            if (tx_res.contains("error"))
                throw std::runtime_error("eth_getTransactionByHash error: " + tx_res["error"].dump());

            json req_rcpt = {
                { "jsonrpc", "2.0" },
                { "method", "eth_getTransactionReceipt" },
                { "params", { hash } },
                { "id", 2 }
            };
            json rcpt_res = json::parse(forward_to_eth_node(req_rcpt.dump()));
            if (rcpt_res.contains("error"))
                throw std::runtime_error("eth_getTransactionReceipt error: " + rcpt_res["error"].dump());

            return std::make_pair(tx_res["result"], rcpt_res["result"]);
        }();

        if (!tx.contains("blockNumber") || tx["blockNumber"].is_null()) {
            // pending tx - shouldn't happen for stored logs
            continue;
        }

        std::string blk_hex = tx["blockNumber"].get<std::string>();
        uint64_t ts = 0;
        auto it = block_ts_cache.find(blk_hex);
        if (it == block_ts_cache.end()) {
            // fetch timestamp (reuse forward_to_eth_node)
            json req_ts = {
                { "jsonrpc", "2.0" },
                { "method", "eth_getBlockByNumber" },
                { "params", { blk_hex, false } },
                { "id", 1 }
            };
            json ts_res = json::parse(forward_to_eth_node(req_ts.dump()));
            if (ts_res.contains("error"))
                throw std::runtime_error("eth_getBlockByNumber error: " + ts_res["error"].dump());
            std::string ts_hex = ts_res["result"]["timestamp"];
            ts = std::stoull(ts_hex.substr(2), nullptr, 16);
            block_ts_cache.emplace(blk_hex, ts);
        } else {
            ts = it->second;
        }

        items.push_back({ hash, std::move(tx), std::move(receipt), blk_hex, ts });
    }

    // 3) newest first (reverse chronological)
    std::reverse(items.begin(), items.end());

    // 4) blocks summary
    json blocksSummary = json::object();
    for (const auto& kv : block_ts_cache) {
        blocksSummary[kv.first] = {
            { "blockNumber", kv.first },
            { "timestamp", kv.second }
        };
    }

    // 5) final result array
    json results = json::array();
    for (const auto& it : items) {
        results.push_back({ { "hash", it.hash },
            { "transaction", it.tx },
            { "receipt", it.receipt } });
    }

    return json {
        { "blocksSummary", std::move(blocksSummary) },
        { "results", std::move(results) }
    };
}

// Thin wrappers for the concrete method names
static json handle_getERC20TransferCount(EventDB& db, const json& p)
{
    return handle_getTransferCount_generic(db, p, "E");
}
static json handle_getERC721TransferCount(EventDB& db, const json& p)
{
    return handle_getTransferCount_generic(db, p, "N");
}
static json handle_getERC20TransferList(EventDB& db, const json& p)
{
    return handle_getTransferList_generic(db, p, "E");
}
static json handle_getERC721TransferList(EventDB& db, const json& p)
{
    return handle_getTransferList_generic(db, p, "N");
}
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

// Method name to handler function mapping
using HandlerFunc = std::function<json(EventDB&, const json&)>;
const std::unordered_map<std::string, HandlerFunc> methodHandlers = {
    { "ots2_getERC20TransferCount", handle_getERC20TransferCount },
    { "ots2_getERC20TransferList", handle_getERC20TransferList },
    { "ots2_getERC721TransferCount", handle_getERC721TransferCount },
    { "ots2_getERC721TransferList", handle_getERC721TransferList },
    { "ots2_getERC20Holdings", handle_getHoldings }
};

auto make_error(const json& id, int code, const std::string& msg)
{
    return json {
        { "jsonrpc", "2.0" },
        { "id", id },
        { "error", { { "code", code }, { "message", msg } } }
    };
};

std::pair<bool, json> try_handle_custom_method(EventDB& db, const json& id,
    const std::string& method,
    const json& params)
{
    auto it = methodHandlers.find(method);
    if (it == methodHandlers.end()) {
        // Not a custom method
        return { false, json() };
    }

    try {
        json result = it->second(db, params);
        return { true, json { { "jsonrpc", "2.0" }, { "id", id }, { "result", std::move(result) } } };
    } catch (const std::invalid_argument& e) {
        return { true, make_error(id, -32602, e.what()) };
    } catch (const std::runtime_error& e) {
        return { true, make_error(id, -32000, e.what()) };
    } catch (const std::exception& e) {
        return { true, make_error(id, -32603, std::string("Internal error: ") + e.what()) };
    }
};

// RPC server runner (starts the HTTP gateway)
static void run_rpc_server(EventDB& db,
    uint16_t listen_port)
{
    httplib::Server svr;

    // expose the server pointer for the signal handler
    g_server_ptr.store(&svr);

    // OPTIONS - CORS pre-flight
    svr.Options("/", [&](const httplib::Request&, httplib::Response& res) {
        set_cors_headers(res);
        res.set_content("", "text/plain");
    });

    // POST - JSON-RPC handling (custom methods + proxy)
    svr.Post("/", [&](const httplib::Request& req, httplib::Response& res) {
        set_cors_headers(res);

        json root;
        try {
            root = json::parse(req.body);
        } catch (...) {
            json err = {
                { "jsonrpc", "2.0" },
                { "id", nullptr },
                { "error", { { "code", -32700 }, { "message", "Parse error" } } }
            };
            res.set_content(err.dump(), "application/json");
            return;
        }

        // Single request
        if (!root.is_array()) {
            json request = root;
            json response = {
                { "jsonrpc", "2.0" },
                { "id", request.value("id", json(nullptr)) }
            };

            std::string method = request.value("method", "");
            json params = request.value("params", json::array());

            auto [handled, custom_response] = try_handle_custom_method(
                db, response["id"], method, params);

            if (handled) {
                response = custom_response;
            } else {
                // Proxy non-custom methods
                std::string forwarded = forward_to_eth_node(req.body);
                res.set_content(forwarded, "application/json");
                return;
            }

            res.set_content(response.dump(), "application/json");
            return;
        }

        // Batch request
        std::vector<json> custom_responses(root.size(), nullptr);
        std::vector<json> proxy_requests;

        for (size_t i = 0; i < root.size(); ++i) {
            const json& req_obj = root[i];
            if (!req_obj.is_object()) {
                custom_responses[i] = make_error(nullptr, -32600, "Invalid Request object");
                continue;
            }

            json id = req_obj.value("id", json(nullptr));
            std::string method = req_obj.value("method", "");
            json params = req_obj.value("params", json::array());

            auto [handled, custom_response] = try_handle_custom_method(
                db, id, method, params);

            if (handled) {
                custom_responses[i] = custom_response;
            } else {
                proxy_requests.push_back(req_obj);
            }
        }

        // Proxy the non-custom calls as a single batch
        std::unordered_map<json, json> proxy_responses_by_id;
        if (!proxy_requests.empty()) {
            std::string batch_body = json(proxy_requests).dump();
            std::string forwarded = forward_to_eth_node(batch_body);
            json batch_resp = json::parse(forwarded);
            if (!batch_resp.is_array())
                throw std::runtime_error("Ethereum node returned non-array batch response");
            for (const auto& resp_obj : batch_resp) {
                json resp_id = resp_obj.value("id", json(nullptr));
                proxy_responses_by_id[resp_id] = resp_obj;
            }
        }

        // Assemble final batch response preserving order
        json final_resp = json::array();
        for (size_t i = 0; i < root.size(); ++i) {
            if (!custom_responses[i].is_null()) {
                final_resp.push_back(custom_responses[i]);
                continue;
            }
            const json& orig = root[i];
            json id = orig.value("id", json(nullptr));
            auto it = proxy_responses_by_id.find(id);
            if (it != proxy_responses_by_id.end())
                final_resp.push_back(it->second);
            else
                final_resp.push_back(make_error(id, -32603,
                    "Missing response from proxy"));
        }

        res.set_content(final_resp.dump(), "application/json");
    });

    // --------------------------------------------------------------------
    std::cout << "Otterscan2 RPC gateway listening on 127.0.0.1:" << listen_port
              << "\nExecution node URL: " << g_rpc_url << '\n';
    std::cout << "Custom methods: ots2_getERC20TransferCount, "
                 "ots2_getERC20TransferList, "
                 "ots2_getERC721TransferCount, "
                 "ots2_getERC721TransferList, "
                 "ots2_getERC20Holdings\n";

    if (!svr.listen("127.0.0.1", listen_port)) {
        std::cerr << "Failed to bind to port " << listen_port << "\n";
        std::exit(EXIT_FAILURE);
    }
}

int main(int argc, char* argv[])
{
    CLI::App app {
        "Otterscan2 - combined syncer + RPC gateway.\n"
        "Runs the log-syncer in the background while exposing the JSON-RPC "
        "gateway on the same process.  Both parts share a single LevelDB "
        "instance."
    };

    std::filesystem::path db_path;
    app.add_option("db_path", db_path,
           "Path to the LevelDB folder (will be created if missing)")
        ->required();

    // Sync-related options
    size_t workers = 32;
    uint64_t batch_range = 64;
    int retry_delay = 5;
    std::string rpc_url = "http://localhost:8545";

    app.add_option("-w,--workers", workers,
           "Number of parallel RPC workers")
        ->check(CLI::PositiveNumber)
        ->default_val(workers);
    app.add_option("-b,--batch-range", batch_range,
           "Blocks per RPC request")
        ->check(CLI::PositiveNumber)
        ->default_val(batch_range);
    app.add_option("-r,--retry-delay", retry_delay,
           "Delay in seconds before retrying a failed batch")
        ->check(CLI::PositiveNumber)
        ->default_val(retry_delay);
    app.add_option("-u,--rpc-url", rpc_url,
           "Execution node JSON-RPC URL")
        ->default_val(rpc_url);

    // RPC-gateway options
    uint16_t listen_port = 17444;
    app.add_option("-p,--port", listen_port,
           "Port on which the gateway will listen")
        ->check(CLI::PositiveNumber)
        ->default_val(listen_port);

    app.set_version_flag("-v,--version", "otterscan2 1.0");

    try {
        app.parse(argc, argv);
    } catch (const CLI::ParseError& e) {
        return app.exit(e);
    }

    // Global URL for the gateway (used by forward_to_eth_node)
    g_rpc_url = rpc_url;

    std::signal(SIGINT, signal_handler);
    std::signal(SIGTERM, signal_handler);

    EventDB event_db(db_path);

    // Start sync thread
    std::thread sync_thread(run_sync_loop,
        std::ref(event_db),
        db_path,
        rpc_url,
        workers,
        batch_range,
        retry_delay);

    // Start RPC server (this thread blocks)
    run_rpc_server(event_db, listen_port);

    // When the HTTP server stops (e.g. due to SIGINT) we join the sync thread.
    if (sync_thread.joinable())
        sync_thread.join();

    // Never reached under normal operation
    return EXIT_SUCCESS;
}
