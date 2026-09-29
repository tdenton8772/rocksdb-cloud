//  Copyright (c) 2026-present, NAM.  All rights reserved.
//
// Internals of LocalSstCache (include/rocksdb/cloud/local_sst_cache.h) and the
// read-through file it hands RocksDB under LocalSstFileMode::kReadThroughCache.
//
// Ownership and locking:
//   * The cache owns one Slot per local SST path it has seen. A Slot holds the
//     local reader, if the file is local, and is shared with every open
//     ReadThroughFile for that path.
//   * Lock order is always cache mutex -> slot mutex. Readers take only the
//     slot mutex, and only to copy a shared_ptr, so a read never blocks on
//     eviction or admission.
//   * Eviction drops the slot's local reader and unlinks the file. A reader
//     already inside Read() holds its own shared_ptr copy of the old reader, so
//     it finishes on the old descriptor; POSIX keeps the inode until the last
//     descriptor closes, which is when the space is actually freed.
#pragma once

#include <atomic>
#include <condition_variable>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "rocksdb/cloud/cloud_storage_provider.h"
#include "rocksdb/cloud/local_sst_cache.h"
#include "rocksdb/file_system.h"

namespace ROCKSDB_NAMESPACE {

class LocalSstCacheImpl : public LocalSstCache,
                          public std::enable_shared_from_this<LocalSstCacheImpl> {
 public:
  enum class State { kRemote, kDownloading, kResident };

  struct Slot {
    explicit Slot(std::string p, std::shared_ptr<FileSystem> fs)
        : path(std::move(p)), base_fs(std::move(fs)) {}
    const std::string path;
    const std::shared_ptr<FileSystem> base_fs;

    std::mutex mu;                               // guards everything below
    State state = State::kRemote;                // written under cache mutex too
    uint64_t bytes = 0;                          // resident or reserved size
    std::shared_ptr<FSRandomAccessFile> local;   // opened lazily when resident
    FileOptions local_opts;
    bool removed = false;                        // RocksDB deleted the file
    const void* owner = nullptr;                 // CloudFileSystemImpl

    std::atomic<int64_t> last_access_ms{0};
    std::atomic<int64_t> last_admit_attempt_ms{0};
    // Reads since the last halving (frequency_admission).
    std::atomic<uint32_t> hits{0};
  };

  explicit LocalSstCacheImpl(const LocalSstCacheOptions& opts);
  ~LocalSstCacheImpl() override;

  // LocalSstCache
  LocalSstCacheStats GetStats() const override;
  const LocalSstCacheOptions& GetOptions() const override { return opts_; }
  void EvictNow() override;

  // The slot for `path`, created on first use.
  std::shared_ptr<Slot> Acquire(const std::string& path,
                                const std::shared_ptr<FileSystem>& base_fs,
                                const void* owner);

  // A complete local copy exists at `path` (written by this node after upload,
  // or found on disk at open). Accounts it and enforces the budget, which may
  // evict it straight away if it does not fit.
  void RegisterResident(const std::string& path, uint64_t bytes,
                        const std::shared_ptr<FileSystem>& base_fs,
                        const void* owner);

  // Rate limit for admission attempts per file
  // (LocalSstCacheOptions::admission_retry_interval). True means try now.
  bool AdmitGateOpen(Slot& slot) const;

  // Ask to bring `slot` local. Returns true if a download was started.
  // `download` runs on a cache worker thread and must leave the complete file
  // at slot->path (write elsewhere, then rename) or fail.
  bool MaybeAdmit(const std::shared_ptr<Slot>& slot, uint64_t bytes,
                  std::function<IOStatus()> download);

  // The local reader for a resident slot, opening it on first use. nullptr if
  // the slot is not resident (the caller then reads remotely).
  std::shared_ptr<FSRandomAccessFile> LocalReader(Slot& slot);

  // RocksDB deleted the file. Drops accounting; the caller deletes the file.
  void Remove(const std::string& path);

  // A CloudFileSystemImpl is going away: drop its queued downloads and wait
  // for its running ones, so no task touches it afterwards.
  void ForgetOwner(const void* owner);

  // One read of `slot`, served locally or not.
  void NoteRead(Slot& slot, bool local) {
    slot.last_access_ms.store(NowMs(), std::memory_order_relaxed);
    slot.hits.fetch_add(1, std::memory_order_relaxed);
    reads_since_halving_.fetch_add(1, std::memory_order_relaxed);
    (local ? reads_local_ : reads_remote_)
        .fetch_add(1, std::memory_order_relaxed);
  }

  static int64_t NowMs();

 private:
  struct Task {
    std::shared_ptr<Slot> slot;
    std::function<IOStatus()> download;
    const void* owner;
  };

  enum class Room { kFits, kNoSpace, kColder };

  // Make `needed` more bytes fit under capacity by evicting least recently
  // used resident slots, down to the eviction target. With a `candidate` and
  // frequency_admission, only slots read less often than the candidate may
  // go; if the room cannot be made from those, nothing is evicted and the
  // result is kColder. Requires mu_.
  Room MakeRoomLocked(uint64_t needed, const Slot* candidate);
  // Halve every slot's hits once enough reads have passed. Requires mu_.
  void MaybeHalveLocked();
  void EvictSlotLocked(const std::shared_ptr<Slot>& slot, bool ttl);
  void TtlSweepLocked(int64_t now_ms);
  void WorkerLoop();
  void SweeperLoop();

  const LocalSstCacheOptions opts_;

  mutable std::mutex mu_;
  std::unordered_map<std::string, std::shared_ptr<Slot>> slots_;
  uint64_t resident_bytes_ = 0;
  uint64_t reserved_bytes_ = 0;
  uint64_t resident_files_ = 0;
  int in_flight_ = 0;              // queued + running downloads
  std::deque<Task> queue_;
  std::vector<const void*> running_owners_;
  std::condition_variable work_cv_;
  std::condition_variable done_cv_;
  std::condition_variable sweep_cv_;
  bool stopping_ = false;

  std::vector<std::thread> workers_;
  std::thread sweeper_;

  // counters
  uint64_t downloads_started_ = 0;
  uint64_t downloads_completed_ = 0;
  uint64_t downloads_failed_ = 0;
  uint64_t refused_space_ = 0;
  uint64_t refused_slots_ = 0;
  uint64_t refused_colder_ = 0;
  std::atomic<uint64_t> reads_since_halving_{0};
  uint64_t evictions_ = 0;
  uint64_t evicted_bytes_ = 0;
  uint64_t ttl_evictions_ = 0;
  std::atomic<uint64_t> reads_local_{0};
  std::atomic<uint64_t> reads_remote_{0};
};

// The FSRandomAccessFile RocksDB gets for every SST under kReadThroughCache,
// local or not, so that eviction can always take the local copy away from it.
class ReadThroughFile : public FSRandomAccessFile {
 public:
  using RemoteFactory =
      std::function<IOStatus(std::unique_ptr<CloudStorageReadableFile>*)>;
  using DownloadFactory = std::function<std::function<IOStatus()>()>;

  ReadThroughFile(std::shared_ptr<LocalSstCacheImpl> cache,
                  std::shared_ptr<LocalSstCacheImpl::Slot> slot,
                  uint64_t file_size, RemoteFactory remote_factory,
                  DownloadFactory download_factory);

  IOStatus Read(uint64_t offset, size_t n, const IOOptions& options,
                Slice* result, char* scratch,
                IODebugContext* dbg) const override;
  IOStatus Prefetch(uint64_t offset, size_t n, const IOOptions& options,
                    IODebugContext* dbg) override;
  IOStatus GetFileSize(uint64_t* result) override {
    *result = file_size_;
    return IOStatus::OK();
  }
  // No stable identity across the local/remote switch.
  size_t GetUniqueId(char* /*id*/, size_t /*max_size*/) const override {
    return 0;
  }
  IOStatus InvalidateCache(size_t /*offset*/, size_t /*length*/) override {
    return IOStatus::OK();
  }

 private:
  // The remote reader, created on first need (a resident file never needs it).
  IOStatus Remote(std::shared_ptr<FSRandomAccessFile>* out) const;
  void MaybeAdmit() const;

  std::shared_ptr<LocalSstCacheImpl> cache_;
  std::shared_ptr<LocalSstCacheImpl::Slot> slot_;
  const uint64_t file_size_;
  RemoteFactory remote_factory_;
  DownloadFactory download_factory_;

  mutable std::mutex remote_mu_;
  mutable std::shared_ptr<FSRandomAccessFile> remote_;
};

}  // namespace ROCKSDB_NAMESPACE
