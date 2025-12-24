// Copyright (C) 2025 Category Labs, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

#include <category/core/assert.h>
#include <category/core/byte_string.hpp>
#include <category/core/keccak.hpp>
#include <category/core/small_prng.hpp>
#include <category/mpt/db.hpp>
#include <category/mpt/ondisk_db_config.hpp>
#include <category/mpt/update.hpp>
#include <category/mpt/util.hpp>
#include <category/mpt/test/test_fixtures_base.hpp>

#include <CLI/CLI.hpp>
#include <quill/Quill.h>

#include <chrono>
#include <filesystem>
#include <iostream>
#include <vector>
#include <numeric>
#include <list>
#include <map>
#include <iomanip>

using namespace monad::mpt;
using namespace monad::test;

// Helper to create a 32-byte hash from a string
static monad::byte_string hash_string(std::string_view s) {
    auto hash = monad::keccak256(monad::byte_string_view{reinterpret_cast<const uint8_t*>(s.data()), s.size()});
    return monad::byte_string{hash.bytes, sizeof(hash.bytes)};
}

static uint64_t get_dir_size(const std::filesystem::path& path) {
    uint64_t size = 0;
    if (std::filesystem::exists(path)) {
        if (std::filesystem::is_directory(path)) {
            for (const auto& entry : std::filesystem::recursive_directory_iterator(path)) {
                if (entry.is_regular_file()) {
                    size += entry.file_size();
                }
            }
        } else if (std::filesystem::is_regular_file(path)) {
            size = std::filesystem::file_size(path);
        }
    }
    return size;
}

int main(int argc, char** argv) {
    int nAccounts = 100;
    int nSlots = 1000;
    int mModify = 10;
    int kCommit = 50;
    int fileSizeGB = 2;
    std::vector<std::filesystem::path> dbPathList;
    bool clearDB = true;

    CLI::App app{"MonadDB MPT Benchmark"};
    app.add_option("-n", nAccounts, "Number of accounts to create")->default_val(100);
    app.add_option("--slots", nSlots, "Number of slots per account (avg)")->default_val(1000);
    app.add_option("-m", mModify, "Number of accounts to modify")->default_val(10);
    app.add_option("-k", kCommit, "Number of accounts per commit")->default_val(50);
    app.add_option("--size", fileSizeGB, "File size in GB")->default_val(2);
    app.add_option("--db", dbPathList, "Path to database file")->required();
    app.add_flag("--clear", clearDB, "Clear database before starting")->default_val(true);

    try {
        app.parse(argc, argv);
    } catch (const CLI::ParseError &e) {
        return app.exit(e);
    }

    if (dbPathList.empty()) {
        std::cerr << "Database path is required." << std::endl;
        return 1;
    }
    std::filesystem::path dbPath = dbPathList[0];

    if (clearDB) {
        std::cout << "Cleaning up old database at " << dbPath << "..." << std::endl;
        std::filesystem::remove_all(dbPath);
    }

    if (dbPath.has_parent_path()) {
        std::filesystem::create_directories(dbPath.parent_path());
    }

    std::cout << "Starting Quill..." << std::endl;
    quill::start(true);

    std::cout << "Initializing MonadDB at " << dbPath << "..." << std::endl;
    // Setup Database
    monad::test::StateMachineAlwaysMerkle machine{};
    auto const config = monad::mpt::OnDiskDbConfig{
        .append = false, 
        .compaction = true, 
        .dbname_paths = dbPathList,
        .file_size_db = fileSizeGB
    };
    std::cout << "Creating Db object..." << std::endl;
    monad::mpt::Db db{machine, config};
    std::cout << "Db object created." << std::endl;

    monad::small_prng r(42);
    
    uint64_t latest_version = db.get_latest_version();
    Node::SharedPtr root;
    if (latest_version != 0 && latest_version != (uint64_t)-1) {
        root = db.load_root_for_version(latest_version);
    }

    std::vector<monad::byte_string> addrs;
    addrs.reserve(nAccounts);

    std::cout << "Phase 1: Creating " << nAccounts << " accounts with variable slots (avg " << nSlots << ")..." << std::endl;
    auto phase1Start = std::chrono::steady_clock::now();
    int64_t totalSlotsCreated = 0;

    for (int i = 0; i < nAccounts; i += kCommit) {
        int batchEnd = std::min(i + kCommit, nAccounts);
        
        // Use maps to deduplicate keys within a batch
        struct AccountData {
            monad::byte_string value;
            std::map<monad::byte_string, monad::byte_string> slots;
        };
        std::map<monad::byte_string, AccountData> batchData;

        for (int j = i; j < batchEnd; ++j) {
            auto addrHash = hash_string("account-" + std::to_string(j));
            if (addrs.size() <= (size_t)j) addrs.push_back(addrHash);

            auto& acc = batchData[addrHash];
            // Simulate account data
            acc.value = monad::byte_string(40, 0);
            for(int b=0; b<40; ++b) acc.value[b] = (uint8_t)(r() % 256);

            int vSlots = r() % (nSlots * 2);
            for (int s = 0; s < vSlots; ++s) {
                auto sKey = hash_string("acc-" + std::to_string(j) + "-slot-" + std::to_string(s));
                monad::byte_string sVal(32, 0);
                uint32_t dice = r() % 100;
                if (dice < 20) {
                } else if (dice < 30) {
                    sVal[31] = 1;
                } else {
                    for(int b=0; b<32; ++b) sVal[b] = (uint8_t)(r() % 256);
                }
                acc.slots[sKey] = sVal;
                totalSlotsCreated++;
            }
        }

        // Now build the intrusive Update structures from the deduplicated data
        std::list<monad::mpt::Update> accountUpdates;
        std::list<monad::mpt::UpdateList> slotUpdateLists;
        std::list<std::list<monad::mpt::Update>> slotUpdatesPerAccount;
        monad::mpt::UpdateList batchUpdateList;

        for (auto& [addr, acc] : batchData) {
            slotUpdateLists.emplace_back();
            auto& currentSlotUpdateList = slotUpdateLists.back();
            slotUpdatesPerAccount.emplace_back();
            auto& currentAccountSlots = slotUpdatesPerAccount.back();

            for (auto const& [sKey, sVal] : acc.slots) {
                currentAccountSlots.push_back(monad::mpt::make_update(sKey, sVal));
                currentSlotUpdateList.push_front(currentAccountSlots.back());
            }

            accountUpdates.push_back(monad::mpt::make_update(addr, acc.value));
            accountUpdates.back().next = std::move(currentSlotUpdateList);
            batchUpdateList.push_front(accountUpdates.back());
        }

        uint64_t version = (uint64_t)(i / kCommit) + 1;
        root = db.upsert(std::move(root), std::move(batchUpdateList), version);
        
        std::cout << "[Batch " << version << "] Disk: " 
                  << std::fixed << std::setprecision(2) << (double)get_dir_size(dbPath) / 1024 / 1024 << " MB" << std::endl;
    }

    auto p1Elapsed = std::chrono::steady_clock::now() - phase1Start;
    double p1Sec = std::chrono::duration<double>(p1Elapsed).count();
    std::cout << "\nCreation finished in " << p1Sec << "s." << std::endl;
    std::cout << "Total Slots Created: " << totalSlotsCreated << " | Throughput: " << (double)totalSlotsCreated / p1Sec << " slots/s" << std::endl;

    // Phase 2: Modification
    mModify = std::min(mModify, nAccounts);
    std::cout << "\nPhase 2: Randomly modifying slots in " << mModify << " accounts..." << std::endl;
    auto phase2Start = std::chrono::steady_clock::now();
    int64_t totalSlotsModified = 0;

    std::vector<int> indices(nAccounts);
    std::iota(indices.begin(), indices.end(), 0);
    // Shuffle indices
    for (int i = nAccounts - 1; i > 0; --i) {
        std::swap(indices[i], indices[r() % (i + 1)]);
    }

    for (int i = 0; i < mModify; i += kCommit) {
        int batchEnd = std::min(i + kCommit, mModify);

        // Deduplicate slots within a batch using a map
        std::map<monad::byte_string, std::map<monad::byte_string, monad::byte_string>> batchModData;

        for (int j = i; j < batchEnd; ++j) {
            int accountIdx = indices[j];
            auto addrHash = addrs[accountIdx];

            for (int s = 0; s < 100; ++s) {
                int slotIdx = r() % nSlots;
                auto sKey = hash_string("acc-" + std::to_string(accountIdx) + "-slot-" + std::to_string(slotIdx));
                
                monad::byte_string sVal(32, 0);
                for(int b=0; b<32; ++b) sVal[b] = (uint8_t)(r() % 256);
                
                batchModData[addrHash][sKey] = sVal;
                totalSlotsModified++;
            }
        }

        // Now build the intrusive Update structures
        std::list<monad::mpt::Update> accountUpdates;
        std::list<monad::mpt::UpdateList> slotUpdateLists;
        std::list<std::list<monad::mpt::Update>> slotUpdatesPerAccount;
        monad::mpt::UpdateList batchUpdateList;

        for (auto& [addr, slots] : batchModData) {
            slotUpdateLists.emplace_back();
            auto& currentSlotUpdateList = slotUpdateLists.back();
            slotUpdatesPerAccount.emplace_back();
            auto& currentAccountSlots = slotUpdatesPerAccount.back();

            for (auto const& [sKey, sVal] : slots) {
                currentAccountSlots.push_back(monad::mpt::make_update(sKey, sVal));
                currentSlotUpdateList.push_front(currentAccountSlots.back());
            }

            accountUpdates.push_back(monad::mpt::make_update(addr, std::move(currentSlotUpdateList)));
            batchUpdateList.push_front(accountUpdates.back());
        }

        uint64_t version = 1000000 + (uint64_t)(i / kCommit) + 1;
        root = db.upsert(std::move(root), std::move(batchUpdateList), version);
        std::cout << "[Mod Batch] committed. Disk: " << (double)get_dir_size(dbPath) / 1024 / 1024 << " MB" << std::endl;
    }

    auto p2Elapsed = std::chrono::steady_clock::now() - phase2Start;
    double p2Sec = std::chrono::duration<double>(p2Elapsed).count();
    std::cout << "\nModification finished in " << p2Sec << "s." << std::endl;
    std::cout << "Total Slots Modified: " << totalSlotsModified << " | Throughput: " << (double)totalSlotsModified / p2Sec << " slots/s" << std::endl;

    std::cout << "\n--- Final Report ---" << std::endl;
    std::cout << "Database Path: " << dbPath << std::endl;
    std::cout << "Disk Usage:    " << (double)get_dir_size(dbPath) / 1024 / 1024 << " MB" << std::endl;

    return 0;
}
