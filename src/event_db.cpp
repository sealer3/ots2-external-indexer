#include "event_db.hpp"

#include <algorithm>
#include <cstring>
#include <iostream>
#include <stdexcept>

using namespace std::literals;
namespace eventdb {

// LevelDB configuration
// 128 MiB write buffer
constexpr std::size_t DEFAULT_WRITE_BUFFER = 128 << 20;
// 1 GiB block cache
constexpr std::size_t DEFAULT_BLOCK_CACHE = 1 << 30;

namespace detail {

    // ---------- LEB128 ----------
    inline void encode_uint64_leb128(std::string& out, std::uint64_t v)
    {
        while (v >= 0x80) {
            out.push_back(static_cast<char>((v & 0x7F) | 0x80));
            v >>= 7;
        }
        out.push_back(static_cast<char>(v));
    }

    inline const char* decode_uint64_leb128(const char* p,
        const char* limit,
        std::uint64_t* out)
    {
        std::uint64_t result = 0;
        int shift = 0;
        while (p < limit) {
            unsigned char byte = static_cast<unsigned char>(*p++);
            result |= static_cast<std::uint64_t>(byte & 0x7F) << shift;
            if ((byte & 0x80) == 0) {
                *out = result;
                return p;
            }
            shift += 7;
            if (shift > 63)
                // overflow
                return nullptr;
        }
        // truncated
        return nullptr;
    }

    inline void append_big_endian_uint64(std::string& out, std::uint64_t v)
    {
        for (int i = 7; i >= 0; --i)
            out.push_back(static_cast<char>((v >> (i * 8)) & 0xFF));
    }

    // prefix is the size as LEB128 + raw bytes
    inline std::string encode_prefix(std::string_view data)
    {
        std::string out;
        encode_uint64_leb128(out, data.size());
        out.append(data);
        return out;
    }

    inline std::string make_event_key(std::string_view list_prefix,
        std::string_view address,
        std::uint64_t idx)
    {
        std::string key = std::string(list_prefix);
        key += encode_prefix(address);
        append_big_endian_uint64(key, idx);
        return key;
    }

    inline std::string make_meta_key(std::string_view list_prefix,
        std::string_view address)
    {
        std::string key = "M"s + std::string(list_prefix);
        key += encode_prefix(address);
        return key;
    }

    // For a generic set we store one key per item:
    //   <set_prefix> <leb128(len(address))> address <leb128(len(item))> item
    inline std::string make_set_key(std::string_view set_prefix,
        std::string_view address,
        std::string_view item)
    {
        std::string key = std::string(set_prefix);
        key += encode_prefix(address);
        key += encode_prefix(item);
        return key;
    }

} // namespace detail

// Constructor - open or create the db
EventDB::EventDB(const std::filesystem::path& dbPath)
{
    leveldb::Options opts;
    opts.create_if_missing = true;
    opts.error_if_exists = false;
    opts.compression = leveldb::kSnappyCompression;
    opts.write_buffer_size = DEFAULT_WRITE_BUFFER;
    opts.max_open_files = 5'000;
    opts.block_cache = leveldb::NewLRUCache(DEFAULT_BLOCK_CACHE);
    opts.block_size = 4 * 1024; // 4 KB (default)
    opts.max_file_size = 64 * 1024 * 1024; // 64 MB

    leveldb::DB* raw = nullptr;
    leveldb::Status s = leveldb::DB::Open(opts, dbPath.string(), &raw);
    if (!s.ok())
        throw std::runtime_error("Cannot open/create DB: "s + s.ToString());

    db_.reset(raw);
}

// Meta read-write helpers
std::uint64_t EventDB::readMeta(std::string_view listPrefix,
    std::string_view address) const
{
    const std::string metaKey = detail::make_meta_key(listPrefix, address);
    std::string metaVal;
    leveldb::Status s = db_->Get(leveldb::ReadOptions(), metaKey, &metaVal);
    if (s.IsNotFound())
        // never written before
        return 0;
    if (!s.ok())
        throw std::runtime_error("Meta read error: "s + s.ToString());

    const char* p = metaVal.data();
    const char* lim = p + metaVal.size();
    std::uint64_t count = 0;
    if (!detail::decode_uint64_leb128(p, lim, &count))
        throw std::runtime_error("Corrupted meta (bad varint)"s);
    return count;
}

void EventDB::writeMeta(std::string_view listPrefix,
    std::string_view address,
    std::uint64_t newCount)
{
    std::string metaKey = detail::make_meta_key(listPrefix, address);
    std::string newMeta;
    detail::encode_uint64_leb128(newMeta, newCount);

    leveldb::WriteOptions wopt;
    leveldb::Status s = db_->Put(wopt, metaKey, newMeta);
    if (!s.ok())
        throw std::runtime_error("Meta write error: "s + s.ToString());
}

// Generic ordered-list appending
void EventDB::appendToList(std::string_view listPrefix,
    std::string_view address,
    const Blob& blob)
{
    const std::uint64_t count = readMeta(listPrefix, address);
    // new element gets index == count
    const std::uint64_t idx = count;

    const std::string evKey = detail::make_event_key(listPrefix, address, idx);
    const std::string metaKey = detail::make_meta_key(listPrefix, address);

    // Encode new count (count+1) as LEB128
    std::string newMeta;
    detail::encode_uint64_leb128(newMeta, count + 1);

    // Atomic batch write
    leveldb::WriteBatch batch;
    batch.Put(evKey, leveldb::Slice(blob.data(), blob.size()));
    batch.Put(metaKey, newMeta);

    leveldb::WriteOptions wopt;
    leveldb::Status s = db_->Write(wopt, &batch);
    if (!s.ok())
        throw std::runtime_error("Append failed: "s + s.ToString());
}

std::uint64_t EventDB::getListCount(std::string_view listPrefix,
    std::string_view address) const
{
    return readMeta(listPrefix, address);
}

std::vector<EventDB::Blob> EventDB::getRecentFromList(std::string_view listPrefix,
    std::string_view address,
    std::size_t K,
    std::size_t offset) const
{
    std::vector<Blob> result;
    if (K == 0)
        return result;

    const std::uint64_t count = readMeta(listPrefix, address);
    if (count == 0)
        // nothing stored yet
        return result;
    if (offset >= count)
        // offset beyond range
        return result;

    // Index of the newest event we actually want to start from
    std::int64_t startIdx = static_cast<std::int64_t>(count - 1 - offset);
    if (startIdx < 0)
        // safety guard
        return result;

    const std::string prefix = std::string(listPrefix) + detail::encode_prefix(address);
    const std::string startKey = detail::make_event_key(listPrefix,
        address,
        static_cast<std::uint64_t>(startIdx));

    leveldb::ReadOptions ro;
    ro.fill_cache = true;
    std::unique_ptr<leveldb::Iterator> it(db_->NewIterator(ro));

    // Seek to the exact key (or the first greater one) then step back if needed
    it->Seek(startKey);
    if (!it->Valid()) {
        // Past the end, go to last key for prefix
        it->SeekToLast();
    } else {
        if (it->key().ToString() > startKey)
            it->Prev();
    }

    // Walk backwards, collecting up to K items that belong to this address
    for (std::size_t collected = 0;
         collected < K && it->Valid();
         ++collected) {

        leveldb::Slice key = it->key();
        if (!key.starts_with(prefix))
            // We left the address-specific range
            break;

        const leveldb::Slice val = it->value();
        if (val.size() != Blob {}.size()) {
            throw std::runtime_error("Corrupted event value: unexpected size");
        }

        Blob b {};
        std::memcpy(b.data(), val.data(), b.size());
        result.emplace_back(std::move(b));

        it->Prev(); // move to older entry
    }
    return result;
}

// Add to the token holdings set
void EventDB::addHolding(std::string_view address,
    std::string_view token)
{
    const std::string key = detail::make_set_key("H"sv, address, token);
    // Store an empty value since we only care about key existence.
    leveldb::WriteOptions wopt;
    leveldb::Status s = db_->Put(wopt, key, "");
    if (!s.ok())
        throw std::runtime_error("addHolding failed: "s + s.ToString());
}

void EventDB::removeHolding(std::string_view address,
    std::string_view token)
{
    const std::string key = detail::make_set_key("H"sv, address, token);
    leveldb::WriteOptions wopt;
    leveldb::Status s = db_->Delete(wopt, key);
    if (!s.ok() && !s.IsNotFound())
        throw std::runtime_error("removeHolding failed: "s + s.ToString());
}

std::vector<std::string> EventDB::listHoldings(std::string_view address) const
{
    std::vector<std::string> out;

    const std::string prefix = "H" + detail::encode_prefix(address);
    leveldb::ReadOptions ro;
    // Only using for iteration
    ro.fill_cache = false;
    std::unique_ptr<leveldb::Iterator> it(db_->NewIterator(ro));

    for (it->Seek(prefix);
         it->Valid();
         it->Next()) {

        leveldb::Slice key = it->key();
        if (!key.starts_with(prefix))
            // We left the address range
            break;

        // key layout: "H" <addrLen> address <tokLen> token
        // We already consumed the "H" + address part; now decode token.
        const char* p = key.data() + prefix.size();
        const char* lim = key.data() + key.size();

        std::uint64_t tokLen = 0;
        const char* after = detail::decode_uint64_leb128(p, lim, &tokLen);
        if (!after)
            throw std::runtime_error("Corrupted holdings key (bad token length)");
        if (static_cast<std::size_t>(tokLen) > static_cast<std::size_t>(lim - after))
            throw std::runtime_error("Corrupted holdings key (token length exceeds key)");

        std::string token(after, static_cast<std::size_t>(tokLen));
        out.emplace_back(std::move(token));
    }
    return out;
}

} // namespace eventdb
