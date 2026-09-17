// NAM Event Listener -- header-only RocksDB EventListener for flush/compaction observability
// Logs all flush, compaction, and SST file lifecycle events with timestamps and details.
#pragma once

#include <chrono>
#include <cstdio>
#include <ctime>
#include <mutex>
#include <atomic>

#include "rocksdb/listener.h"

namespace nam {

class NamEventListener : public ROCKSDB_NAMESPACE::EventListener {
 public:
  const char* Name() const override { return "NamEventListener"; }

  // --- Counters (thread-safe) ---
  std::atomic<uint64_t> flush_count{0};
  std::atomic<uint64_t> compaction_count{0};
  std::atomic<uint64_t> sst_created_count{0};
  std::atomic<uint64_t> sst_deleted_count{0};
  std::atomic<uint64_t> total_flushed_bytes{0};
  std::atomic<uint64_t> total_compacted_bytes{0};

  void OnFlushBegin(ROCKSDB_NAMESPACE::DB* /*db*/,
                    const ROCKSDB_NAMESPACE::FlushJobInfo& info) override {
    LogTimestamp();
    fprintf(stderr, "[NAM-EVENT] FLUSH_BEGIN  cf=%s  job=%d  reason=%s\n",
            info.cf_name.c_str(), info.job_id,
            ROCKSDB_NAMESPACE::GetFlushReasonString(info.flush_reason));
  }

  void OnFlushCompleted(ROCKSDB_NAMESPACE::DB* /*db*/,
                        const ROCKSDB_NAMESPACE::FlushJobInfo& info) override {
    flush_count.fetch_add(1, std::memory_order_relaxed);
    total_flushed_bytes.fetch_add(info.table_properties.data_size,
                                  std::memory_order_relaxed);
    LogTimestamp();
    fprintf(stderr,
            "[NAM-EVENT] FLUSH_DONE   cf=%s  job=%d  path=%s  "
            "data_size=%lu  index_size=%lu  num_entries=%lu  "
            "smallest_seqno=%lu  largest_seqno=%lu  "
            "writes_slowdown=%d  writes_stop=%d  reason=%s\n",
            info.cf_name.c_str(), info.job_id, info.file_path.c_str(),
            (unsigned long)info.table_properties.data_size,
            (unsigned long)info.table_properties.index_size,
            (unsigned long)info.table_properties.num_entries,
            (unsigned long)info.smallest_seqno,
            (unsigned long)info.largest_seqno,
            info.triggered_writes_slowdown ? 1 : 0,
            info.triggered_writes_stop ? 1 : 0,
            ROCKSDB_NAMESPACE::GetFlushReasonString(info.flush_reason));
  }

  void OnCompactionBegin(ROCKSDB_NAMESPACE::DB* /*db*/,
                         const ROCKSDB_NAMESPACE::CompactionJobInfo& ci) override {
    LogTimestamp();
    fprintf(stderr,
            "[NAM-EVENT] COMPACT_BEGIN  cf=%s  job=%d  "
            "input_level=%d  output_level=%d  input_files=%zu\n",
            ci.cf_name.c_str(), ci.job_id, ci.base_input_level,
            ci.output_level, ci.input_files.size());
  }

  void OnCompactionCompleted(ROCKSDB_NAMESPACE::DB* /*db*/,
                             const ROCKSDB_NAMESPACE::CompactionJobInfo& ci) override {
    compaction_count.fetch_add(1, std::memory_order_relaxed);
    LogTimestamp();
    fprintf(stderr,
            "[NAM-EVENT] COMPACT_DONE  cf=%s  job=%d  status=%s  "
            "input_level=%d  output_level=%d  "
            "input_files=%zu  output_files=%zu  "
            "input_records=%lu  output_records=%lu  "
            "elapsed_us=%lu\n",
            ci.cf_name.c_str(), ci.job_id, ci.status.ToString().c_str(),
            ci.base_input_level, ci.output_level,
            ci.input_files.size(), ci.output_files.size(),
            (unsigned long)ci.stats.num_input_records,
            (unsigned long)ci.stats.num_output_records,
            (unsigned long)ci.stats.elapsed_micros);
    total_compacted_bytes.fetch_add(ci.stats.total_output_bytes,
                                    std::memory_order_relaxed);
  }

  void OnTableFileCreated(
      const ROCKSDB_NAMESPACE::TableFileCreationInfo& info) override {
    sst_created_count.fetch_add(1, std::memory_order_relaxed);
    LogTimestamp();
    fprintf(stderr,
            "[NAM-EVENT] SST_CREATED   path=%s  size=%lu  status=%s  "
            "num_entries=%lu\n",
            info.file_path.c_str(), (unsigned long)info.file_size,
            info.status.ToString().c_str(),
            (unsigned long)info.table_properties.num_entries);
  }

  void OnTableFileDeleted(
      const ROCKSDB_NAMESPACE::TableFileDeletionInfo& info) override {
    sst_deleted_count.fetch_add(1, std::memory_order_relaxed);
    LogTimestamp();
    fprintf(stderr, "[NAM-EVENT] SST_DELETED   path=%s  status=%s\n",
            info.file_path.c_str(), info.status.ToString().c_str());
  }

  void PrintSummary() const {
    fprintf(stderr,
            "\n=== NAM Event Listener Summary ===\n"
            "  Flushes completed:     %lu\n"
            "  Compactions completed: %lu\n"
            "  SST files created:     %lu\n"
            "  SST files deleted:     %lu\n"
            "  Total flushed bytes:   %lu\n"
            "  Total compacted bytes: %lu\n"
            "==================================\n",
            (unsigned long)flush_count.load(),
            (unsigned long)compaction_count.load(),
            (unsigned long)sst_created_count.load(),
            (unsigned long)sst_deleted_count.load(),
            (unsigned long)total_flushed_bytes.load(),
            (unsigned long)total_compacted_bytes.load());
  }

 private:
  void LogTimestamp() const {
    auto now = std::chrono::system_clock::now();
    auto t = std::chrono::system_clock::to_time_t(now);
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                  now.time_since_epoch()) %
              1000;
    struct tm buf;
    localtime_r(&t, &buf);
    fprintf(stderr, "%04d-%02d-%02d %02d:%02d:%02d.%03d ",
            buf.tm_year + 1900, buf.tm_mon + 1, buf.tm_mday, buf.tm_hour,
            buf.tm_min, buf.tm_sec, (int)ms.count());
  }
};

}  // namespace nam
