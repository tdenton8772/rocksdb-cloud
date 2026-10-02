//  Copyright (c) 2026-present, NAM.  All rights reserved.
//
// A process-wide, byte-bounded cache of local SST copies for
// LocalSstFileMode::kReadThroughCache.
//
// The behaviour it provides (the mode Rockset ran):
//
//   * The first read of an SST that is not on local disk is served from cloud
//     storage by range request, and the cache is asked to ADMIT the whole file.
//   * Admission is the disk guard. The whole-file download starts only if the
//     file fits the byte budget after evicting least-recently-used local files,
//     and a download slot is free. Otherwise nothing is downloaded and reads
//     stay remote -- a node that suddenly owns many ranges gets slower, it never
//     fills its disk.
//   * When the download lands, readers of that file switch to the local copy.
//   * Eviction is least-recently-used under the hard byte budget, with an
//     optional TTL on top. Evicting a file that RocksDB still holds open drops
//     that file back to remote reads; in-flight reads finish on the old handle,
//     and the space is freed when the last one closes.
//
// One instance is meant to be shared by every DB in the process, since they
// share one disk: pass the same shared_ptr in every CloudFileSystemOptions.
#pragma once

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>

#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

struct LocalSstCacheOptions {
  // Hard ceiling on bytes of local SST copies this cache accounts for, on-disk
  // downloads in flight included. 0 means cache nothing: every read is remote.
  uint64_t capacity_bytes = 0;

  // When eviction runs it frees down to capacity_bytes * evict_to_fraction, so
  // a steady stream of admissions does not evict one file per admission.
  double evict_to_fraction = 0.9;

  // Evict a local copy that has not been read for this long. 0 disables TTL;
  // the byte budget applies regardless.
  std::chrono::seconds ttl{0};

  // Whole-file downloads allowed in flight at once, process-wide. Bounds the
  // network a node spends warming its cache.
  int max_concurrent_downloads = 2;

  // A file whose admission was refused (no room, or no download slot) is not
  // retried more often than this, however often it is read.
  std::chrono::milliseconds admission_retry_interval{1000};

  // Admission when the budget is full. A file that fits in free space is
  // admitted on its first read. A file that needs room is admitted only if
  // every file it would evict has been read less often than it recently;
  // otherwise nothing is evicted and the read stays remote. Without this, a
  // working set larger than the budget evicts each file just before it is
  // read again and every read turns into a whole-file download. false gives
  // plain LRU admission.
  bool frequency_admission = true;

  // "Recently": every file's read count is halved after this many reads
  // across the cache, so a file that was hot and went cold loses its place.
  // 0 means 16 x the number of files the cache has seen, at least 1024.
  uint64_t frequency_halving_reads = 0;
};

struct LocalSstCacheStats {
  uint64_t capacity_bytes = 0;
  uint64_t resident_bytes = 0;   // local copies on disk
  uint64_t reserved_bytes = 0;   // downloads in flight
  uint64_t resident_files = 0;
  uint64_t downloads_started = 0;
  uint64_t downloads_completed = 0;
  uint64_t downloads_failed = 0;
  uint64_t admissions_refused_space = 0;
  uint64_t admissions_refused_slots = 0;
  // Refused because every file that would have to go is read at least as
  // often (frequency_admission).
  uint64_t admissions_refused_colder = 0;
  uint64_t evictions = 0;
  uint64_t evicted_bytes = 0;
  uint64_t ttl_evictions = 0;
  uint64_t reads_local = 0;
  uint64_t reads_remote = 0;
};

class LocalSstCache {
 public:
  virtual ~LocalSstCache() {}
  virtual LocalSstCacheStats GetStats() const = 0;
  virtual const LocalSstCacheOptions& GetOptions() const = 0;
  // Run TTL and budget eviction now (they also run on their own).
  virtual void EvictNow() = 0;
};

std::shared_ptr<LocalSstCache> NewLocalSstCache(const LocalSstCacheOptions& opts);

}  // namespace ROCKSDB_NAMESPACE
