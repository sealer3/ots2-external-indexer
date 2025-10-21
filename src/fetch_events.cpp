/*
 *  src/fetch_events.cpp - CLI that prints all transaction hashes for an
 *    address stored in EventDB.
 */

#include "event_db.hpp"

#include <CLI/CLI.hpp>
#include <algorithm>
#include <cctype>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

using namespace eventdb;

// Turn a 32-byte blob into a 0x-prefixed hex string
static std::string blob_to_hex(const EventDB::Blob& b)
{
    static const char* hex_chars = "0123456789abcdef";
    std::string out = "0x";
    out.reserve(2 + b.size() * 2);
    for (unsigned char c : b) {
        out.push_back(hex_chars[(c >> 4) & 0xF]);
        out.push_back(hex_chars[c & 0xF]);
    }
    return out;
}

// Retrieve ERC-20 transaction hashes stored for `addr`.
// ERC-20 events are stored in the list with prefix "E".
static std::vector<EventDB::Blob>
fetch_all_erc20_events(EventDB& db,
    const std::string& addr,
    size_t limit)
{
    return db.getRecentFromList("E", addr, limit, 0);
}

// Retrieve ERC-721 transaction hashes stored for `addr`.
// ERC-721 events are stored in the list with prefix "N".
static std::vector<EventDB::Blob>
fetch_all_erc721_events(EventDB& db,
    const std::string& addr,
    size_t limit)
{
    return db.getRecentFromList("N", addr, limit, 0);
}

// CLI-level address validation.
// Returns the address in lowercase if it passes, otherwise throws.
static std::string validate_and_normalize_address(const std::string& raw)
{
    // Must start with "0x" (case-insensitive) and be exactly 42 characters long.
    if (raw.size() != 42 || raw[0] != '0' || (raw[1] != 'x' && raw[1] != 'X')) {
        throw CLI::ValidationError(
            "address must be a 0x-prefixed hex string of 40 characters");
    }

    // Verify that every remaining character is a hex digit.
    for (size_t i = 2; i < raw.size(); ++i) {
        char c = raw[i];
        if (!std::isxdigit(static_cast<unsigned char>(c))) {
            throw CLI::ValidationError(
                "address contains non-hex character at position " + std::to_string(i));
        }
    }

    // Normalize to lowercase (the DB stores everything lowercase).
    std::string lowered = raw;
    std::transform(lowered.begin(), lowered.end(), lowered.begin(),
        [](unsigned char ch) { return static_cast<char>(std::tolower(ch)); });
    return lowered;
}

// Main entry point
int main(int argc, char* argv[])
{
    CLI::App app {
        "CLI that prints recent ERC-20/ERC-721 transaction hashes and token holdings\n"
        "for a given address stored in an EventDB (LevelDB) database."
    };

    // Positional arguments
    std::filesystem::path db_path;
    app.add_option("db_path", db_path,
           "Path to the LevelDB folder (will be created if missing)")
        ->required()
        ->check(CLI::ExistingDirectory);

    std::string address_raw;
    app.add_option("address", address_raw,
           "0x-prefixed hex address (40 hex chars)")
        ->required()
        ->check([](const std::string& v) {
            // We just need to return CLI::Success / CLI::ValidationError
            try {
                (void)validate_and_normalize_address(v);
                // Success
                return std::string {};
            } catch (const CLI::ValidationError& e) {
                // Forward error
                return std::string(e.what());
            }
        });

    // Optional flags
    size_t limit = 25;
    app.add_option("-l,--limit", limit,
           "Maximum number of recent hashes to display per list")
        ->check(CLI::PositiveNumber);

    app.set_version_flag("-v,--version", "fetch_events 1.0");

    // Parse command line
    try {
        app.parse(argc, argv);
    } catch (const CLI::ParseError& e) {
        return app.exit(e);
    }

    // Normalize the address
    const std::string address = validate_and_normalize_address(address_raw);

    // Fetch stored transaction hashes for the supplied address.
    EventDB event_db(db_path);
    std::vector<EventDB::Blob> erc20_blobs = fetch_all_erc20_events(event_db, address, limit);
    std::vector<EventDB::Blob> erc721_blobs = fetch_all_erc721_events(event_db, address, limit);
    std::vector<std::string> holdings = event_db.listHoldings(address);

    // ERC-20 transaction hashes
    if (erc20_blobs.empty()) {
        std::cout << "No ERC-20 events found for address " << address << '\n';
    } else {
        std::cout << "Found " << erc20_blobs.size()
                  << " ERC-20 event(s) for address " << address << ":\n";
        for (const auto& b : erc20_blobs) {
            std::cout << "  " << blob_to_hex(b) << '\n';
        }
    }

    // ERC-721 transaction hashes
    std::cout << '\n';
    if (erc721_blobs.empty()) {
        std::cout << "No ERC-721 events found for address " << address << '\n';
    } else {
        std::cout << "Found " << erc721_blobs.size()
                  << " ERC-721 event(s) for address " << address << ":\n";
        for (const auto& b : erc721_blobs) {
            std::cout << "  " << blob_to_hex(b) << '\n';
        }
    }

    // Token holdings
    std::cout << '\n';
    if (holdings.empty()) {
        std::cout << "No token holdings recorded for address " << address << ".\n";
    } else {
        std::cout << "Address " << address
                  << " holds " << holdings.size()
                  << " ERC-20 token(s):\n";
        for (const std::string& token : holdings) {
            std::cout << "  0x" << token << '\n';
        }
    }

    return EXIT_SUCCESS;
}
