//  event_db.hpp public interface
#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <leveldb/cache.h>
#include <leveldb/db.h>
#include <leveldb/iterator.h>
#include <leveldb/options.h>
#include <leveldb/status.h>
#include <leveldb/write_batch.h>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

using namespace std::literals;

namespace eventdb {

namespace detail {
    // Low-level helpers
    void encode_uint64_leb128(std::string& out, std::uint64_t v);
    const char* decode_uint64_leb128(const char* p,
        const char* limit,
        std::uint64_t* out);
    void append_big_endian_uint64(std::string& out, std::uint64_t v);
    std::string encode_prefix(std::string_view data);
    std::string make_event_key(std::string_view list_prefix,
        std::string_view address,
        std::uint64_t idx);
    std::string make_meta_key(std::string_view list_prefix,
        std::string_view address);
    std::string make_set_key(std::string_view set_prefix,
        std::string_view address,
        std::string_view item);
}

class EventDB {
public:
    // fixed-size payload (still used by the demo)
    using Blob = std::array<char, 32>;

    explicit EventDB(const std::filesystem::path& dbPath);
    EventDB(const EventDB&) = delete;
    EventDB& operator=(const EventDB&) = delete;
    EventDB(EventDB&&) noexcept = default;
    EventDB& operator=(EventDB&&) noexcept = default;

    //  Generic ordered list API
    // Append a new item to a list identified by `listPrefix` for `address`
    void appendToList(std::string_view listPrefix,
        std::string_view address,
        const Blob& blob);

    // Return the number of items stored for `listPrefix` / `address`
    std::uint64_t getListCount(std::string_view listPrefix,
        std::string_view address) const;

    // Retrieve the newest K items from a list (newest to oldest)
    std::vector<Blob> getRecentFromList(std::string_view listPrefix,
        std::string_view address,
        std::size_t K,
        std::size_t offset = 0) const;

    //  Token-holdings set API
    // Record that `address` has interacted with `token`. No-op if already present
    void addHolding(std::string_view address, std::string_view token);

    // Remove a token from the holdings set (optional helper)
    void removeHolding(std::string_view address, std::string_view token);

    // Return the whole set of token-addresses `address` has interacted with
    std::vector<std::string> listHoldings(std::string_view address) const;

private:
    // RAII wrapper around LevelDB instance
    std::unique_ptr<leveldb::DB> db_;

    std::uint64_t readMeta(std::string_view listPrefix,
        std::string_view address) const;
    void writeMeta(std::string_view listPrefix,
        std::string_view address,
        std::uint64_t newCount);
};

} // namespace eventdb
