// NAM Cloud Request Logger -- header-only CloudRequestCallback for S3 operation observability
// Logs every S3 operation with type, size, latency, and success/failure.
#pragma once

#include <atomic>
#include <chrono>
#include <cstdio>
#include <ctime>
#include <mutex>

#include "rocksdb/cloud/cloud_file_system.h"

namespace nam {

// Returns human-readable name for CloudRequestOpType
inline const char* CloudOpName(ROCKSDB_NAMESPACE::CloudRequestOpType op) {
  switch (op) {
    case ROCKSDB_NAMESPACE::CloudRequestOpType::kReadOp:   return "READ";
    case ROCKSDB_NAMESPACE::CloudRequestOpType::kWriteOp:  return "WRITE";
    case ROCKSDB_NAMESPACE::CloudRequestOpType::kListOp:   return "LIST";
    case ROCKSDB_NAMESPACE::CloudRequestOpType::kCreateOp: return "CREATE";
    case ROCKSDB_NAMESPACE::CloudRequestOpType::kDeleteOp: return "DELETE";
    case ROCKSDB_NAMESPACE::CloudRequestOpType::kCopyOp:   return "COPY";
    case ROCKSDB_NAMESPACE::CloudRequestOpType::kInfoOp:   return "INFO";
    default: return "UNKNOWN";
  }
}

struct CloudRequestStats {
  std::atomic<uint64_t> read_ops{0};
  std::atomic<uint64_t> write_ops{0};
  std::atomic<uint64_t> list_ops{0};
  std::atomic<uint64_t> create_ops{0};
  std::atomic<uint64_t> delete_ops{0};
  std::atomic<uint64_t> copy_ops{0};
  std::atomic<uint64_t> info_ops{0};
  std::atomic<uint64_t> total_bytes{0};
  std::atomic<uint64_t> total_latency_us{0};
  std::atomic<uint64_t> failures{0};

  void Record(ROCKSDB_NAMESPACE::CloudRequestOpType op, uint64_t size,
              uint64_t latency_us, bool success) {
    switch (op) {
      case ROCKSDB_NAMESPACE::CloudRequestOpType::kReadOp:
        read_ops.fetch_add(1, std::memory_order_relaxed); break;
      case ROCKSDB_NAMESPACE::CloudRequestOpType::kWriteOp:
        write_ops.fetch_add(1, std::memory_order_relaxed); break;
      case ROCKSDB_NAMESPACE::CloudRequestOpType::kListOp:
        list_ops.fetch_add(1, std::memory_order_relaxed); break;
      case ROCKSDB_NAMESPACE::CloudRequestOpType::kCreateOp:
        create_ops.fetch_add(1, std::memory_order_relaxed); break;
      case ROCKSDB_NAMESPACE::CloudRequestOpType::kDeleteOp:
        delete_ops.fetch_add(1, std::memory_order_relaxed); break;
      case ROCKSDB_NAMESPACE::CloudRequestOpType::kCopyOp:
        copy_ops.fetch_add(1, std::memory_order_relaxed); break;
      case ROCKSDB_NAMESPACE::CloudRequestOpType::kInfoOp:
        info_ops.fetch_add(1, std::memory_order_relaxed); break;
    }
    total_bytes.fetch_add(size, std::memory_order_relaxed);
    total_latency_us.fetch_add(latency_us, std::memory_order_relaxed);
    if (!success) failures.fetch_add(1, std::memory_order_relaxed);
  }

  void Print() const {
    fprintf(stderr,
            "\n=== NAM Cloud Request Stats ===\n"
            "  READ:   %lu ops\n"
            "  WRITE:  %lu ops\n"
            "  LIST:   %lu ops\n"
            "  CREATE: %lu ops\n"
            "  DELETE: %lu ops\n"
            "  COPY:   %lu ops\n"
            "  INFO:   %lu ops\n"
            "  Total bytes:      %lu\n"
            "  Total latency:    %lu us\n"
            "  Failures:         %lu\n"
            "===============================\n",
            (unsigned long)read_ops.load(),
            (unsigned long)write_ops.load(),
            (unsigned long)list_ops.load(),
            (unsigned long)create_ops.load(),
            (unsigned long)delete_ops.load(),
            (unsigned long)copy_ops.load(),
            (unsigned long)info_ops.load(),
            (unsigned long)total_bytes.load(),
            (unsigned long)total_latency_us.load(),
            (unsigned long)failures.load());
  }
};

// Creates a CloudRequestCallback that logs every S3 op and accumulates stats.
// Usage:
//   auto stats = std::make_shared<nam::CloudRequestStats>();
//   auto cb = nam::MakeCloudRequestLogger(stats);
//   cloud_fs_options.cloud_request_callback = cb;
inline std::shared_ptr<ROCKSDB_NAMESPACE::CloudRequestCallback>
MakeCloudRequestLogger(std::shared_ptr<CloudRequestStats> stats,
                       bool verbose = true) {
  return std::make_shared<ROCKSDB_NAMESPACE::CloudRequestCallback>(
      [stats, verbose](ROCKSDB_NAMESPACE::CloudRequestOpType op,
                       uint64_t size, uint64_t latency_us, bool success) {
        stats->Record(op, size, latency_us, success);
        if (verbose) {
          fprintf(stderr,
                  "[NAM-S3] %-6s  size=%8lu  latency=%6lu us  %s\n",
                  CloudOpName(op), (unsigned long)size,
                  (unsigned long)latency_us,
                  success ? "OK" : "FAILED");
        }
      });
}

}  // namespace nam
