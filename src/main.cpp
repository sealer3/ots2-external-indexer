// main.cpp - Erigon log sync -> LevelDB event store

#include "event_db.hpp"
#include "rpc.hpp"

#include <CLI/CLI.hpp>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <csignal>
#include <filesystem>
#include <fstream>
#include <future>
#include <iostream>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

using namespace eventdb;

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

// Global interrupt handling
static std::atomic<bool> g_terminate { false };

static void signal_handler(int /*signum*/)
{
    g_terminate.store(true);
}

// Helper: libcurl write callback -> std::string
static std::string strip_0x(const std::string& s)
{
    return (s.size() >= 2 && s[0] == '0' && (s[1] == 'x' || s[1] == 'X')) ? s.substr(2) : s;
}

// Decode a 0x-prefixed hex string into a 32-byte Blob (char array).
static std::optional<EventDB::Blob> hex_to_blob(const std::string& hex_str)
{
    std::string h = strip_0x(hex_str);
    if (h.size() != 64)
        // 32 bytes == 64 hex chars
        return std::nullopt;
    EventDB::Blob out {};
    for (size_t i = 0; i < 32; ++i) {
        unsigned int byte {};
        std::sscanf(h.data() + i * 2, "%2x", &byte);
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

// Persistent storage of the "last synced block".
static const std::string kMetaFileName = "last_block.txt";

// Read the last-saved block from the meta file.
// If the file does not exist or cannot be parsed we assume block 0.
static uint64_t read_last_block_file(const std::filesystem::path& db_dir)
{
    std::filesystem::path meta_path = db_dir / kMetaFileName;
    std::ifstream in(meta_path);
    if (!in) {
        // No file -> fresh start
        return 0;
    }

    uint64_t blk = 0;
    in >> blk;
    if (in.fail()) {
        std::cerr << "Warning: could not parse '" << meta_path
                  << "'. Starting from block 0.\n";
        return 0;
    }
    return blk;
}

// Write the latest block number to the meta file (overwrites previous value).
static void write_last_block_file(const std::filesystem::path& db_dir,
    uint64_t blk)
{
    std::filesystem::path meta_path = db_dir / kMetaFileName;
    std::ofstream out(meta_path, std::ios::trunc);
    if (!out) {
        std::cerr << "Error: cannot write last block to '" << meta_path << "'\n";
        return;
    }
    out << blk << '\n';
}

// Main sync loop
int main(int argc, char* argv[])
{
    // CLI11 parsing
    CLI::App app {
        "Otterscan2 external indexer\n"
        "Synchronizes ERC-20/-721 Transfer logs from an Erigon node and stores them "
        "in a LevelDB-based event DB.\n"
        "Exit cleanly by sending an interrupt (Ctrl+C)."
    };

    // Positional argument: the DB folder (required)
    std::filesystem::path db_path;
    app.add_option("db_path", db_path,
           "Path to the LevelDB folder (will be created if missing)")
        ->required();

    size_t workers = 32;
    uint64_t batch_range = 64;
    int retry_delay = 5; // seconds
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

    app.set_version_flag("-v,--version", "event_syncer 1.0");

    try {
        app.parse(argc, argv);
    } catch (const CLI::ParseError& e) {
        // CLI11 will already have printed a helpful message
        return app.exit(e);
    }

    // Signal handling
    std::signal(SIGINT, signal_handler);
    std::signal(SIGTERM, signal_handler);

    // Open EventDB
    EventDB event_db(db_path);

    // Resume point
    uint64_t last_saved_block = read_last_block_file(db_path);
    uint64_t next_block = (last_saved_block == 0) ? 0 : last_saved_block + 1;
    std::cout << "Resuming from block " << next_block
              << " (last saved = " << last_saved_block << ")\n";

    // RPC client
    rpc::Client rpc_client(rpc_url);

    const uint64_t kBatchRange = batch_range;
    const size_t kWorkers = workers;
    const uint64_t kMaxBatchSize = static_cast<uint64_t>(kWorkers) * kBatchRange;
    const int kRetryDelaySec = retry_delay;
    const std::string transfer_topic = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";

    // Get tip block
    std::optional<uint64_t> tip_opt;
    do {
        tip_opt = rpc_client.get_latest_block();

        if (!tip_opt) {
            std::cerr << "[main] failed to get latest block, retrying in "
                      << kRetryDelaySec << " s ...\n";
            std::this_thread::sleep_for(std::chrono::seconds(kRetryDelaySec));
        }
    } while (!tip_opt);
    uint64_t tip_block_number = *tip_opt;

    // Sync loop
    while (!g_terminate.load()) {
        if (next_block > tip_block_number) {
            // Nothing new to sync
            std::cout << "Synced! Waiting for new blocks..." << std::endl;
            std::this_thread::sleep_for(std::chrono::seconds(5));

            tip_opt = rpc_client.get_latest_block();
            if (!tip_opt) {
                std::cerr << "[main] failed to get latest block" << std::endl;
            } else {
                tip_block_number = *tip_opt;
                std::cout << "Current block: " << tip_block_number << std::endl;
            }
            continue;
        }

        uint64_t batch_start = next_block;
        uint64_t batch_end = std::min(batch_start + kMaxBatchSize - 1,
            tip_block_number);

        // Fetch the whole batch - repeat until all workers succeed
        bool batch_ok = false;
        std::vector<rpc::Client::RpcResult> results;

        while (!batch_ok && !g_terminate.load()) {
            // launch workers
            std::vector<std::future<rpc::Client::RpcResult>> futures;
            futures.reserve(kWorkers);
            for (size_t i = 0; i < kWorkers; ++i) {
                uint64_t from = batch_start + i * kBatchRange;
                uint64_t to = std::min(from + kBatchRange - 1, batch_end);
                if (from <= to) {
                    futures.emplace_back(
                        std::async(std::launch::async,
                            &rpc::Client::fetch_log_range,
                            &rpc_client,
                            from, to,
                            transfer_topic));
                }
            }

            // Collect results
            results.clear();
            results.reserve(futures.size());
            for (auto& f : futures)
                results.emplace_back(f.get());

            std::sort(results.begin(), results.end(),
                [](const rpc::Client::RpcResult& a,
                    const rpc::Client::RpcResult& b) {
                    return a.firstBlock < b.firstBlock;
                });

            // Check that every worker succeeded
            batch_ok = std::all_of(results.begin(), results.end(),
                [](const rpc::Client::RpcResult& r) { return r.ok; });

            if (!batch_ok) {
                std::cerr << "[Batch " << batch_start << "-" << batch_end
                          << "] some RPC calls failed - retrying in "
                          << kRetryDelaySec << " s ...\n";
                std::this_thread::sleep_for(std::chrono::seconds(kRetryDelaySec));
                // Loop again: we will relaunch all workers for the same range.
            }
        }

        if (g_terminate.load())
            // Clean exit if we were interrupted while retrying
            break;

        // Process the logs
        uint64_t events_appended = 0;
        for (const auto& r : results) {
            // At this point `r.ok` is guaranteed true.
            if (!r.payload.contains("result") || !r.payload["result"].is_array())
                continue;

            std::unordered_map<std::string, std::string> lastTxForAddr20;
            std::unordered_map<std::string, std::string> lastTxForAddr721;
            std::unordered_set<std::pair<std::string, std::string>> addressRecordedTokens;

            for (const auto& log : r.payload["result"]) {
                if (!log.contains("topics") || !log["topics"].is_array())
                    continue;
                const auto& topics = log["topics"];
                if (topics.size() != 3 && topics.size() != 4)
                    // Not ERC-20 or ERC-721
                    continue;

                // Determine token-type specific parameters.
                const bool is_erc20 = (topics.size() == 3);
                const bool is_erc721 = (topics.size() == 4);
                const std::string list_prefix = is_erc721 ? "N" : "E";
                auto& lastTxForAddr = is_erc721 ? lastTxForAddr721 : lastTxForAddr20;

                // extract FROM / TO addresses (last 20 bytes of the topic)
                std::string from_addr = topic_to_address(topics[1].get<std::string>());
                std::string to_addr = topic_to_address(topics[2].get<std::string>());
                if (from_addr.empty() || to_addr.empty())
                    continue;

                // transaction hash -> Blob
                if (!log.contains("transactionHash"))
                    continue;
                const std::string tx_hash_hex = log["transactionHash"].get<std::string>();
                auto blob_opt = hex_to_blob(tx_hash_hex);
                if (!blob_opt)
                    continue;
                const EventDB::Blob& tx_blob = *blob_opt;

                // Append to both parties
                const std::array<std::string, 2> addrs { from_addr, to_addr };
                for (const std::string& addr : addrs) {
                    auto it = lastTxForAddr.find(addr);
                    if (it == lastTxForAddr.end() || it->second != tx_hash_hex) {
                        event_db.appendToList(list_prefix, addr, tx_blob);
                        ++events_appended;
                        // Remember the new hash so we don't have duplicate
                        // transaction hashes in the log for this address
                        lastTxForAddr[addr] = tx_hash_hex;
                    }

                    // Record ERC-20 contract addresses as holdings
                    if (is_erc20 && log.contains("address")) {
                        std::string tokenAddress = strip_0x(log["address"].get<std::string>());
                        auto pair = std::make_pair(addr, tokenAddress);
                        if (addressRecordedTokens.insert(pair).second) {
                            event_db.addHolding(addr, tokenAddress);
                        }
                    }
                }
            }
        }

        // Persist the new "last saved block" only after the whole batch
        // has been successfully written.
        uint64_t new_last_block = batch_end;
        write_last_block_file(db_path, new_last_block);
        last_saved_block = new_last_block;
        next_block = new_last_block + 1;

        std::cout << "[Batch " << batch_start << '-' << batch_end << "] "
                  << "appended " << events_appended << " events. "
                  << "Last saved block = " << last_saved_block << '\n';

        // If we hit the termination flag after a batch, break now.
        if (g_terminate.load())
            break;
    }

    // Graceful shutdown
    std::cout << "Shutting down - final last-saved block = "
              << last_saved_block << '\n';
    // `event_db` destructor will close the DB automatically.
    return EXIT_SUCCESS;
}
