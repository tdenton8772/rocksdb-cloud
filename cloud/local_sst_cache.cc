//  Copyright (c) 2026-present, NAM.  All rights reserved.
//
// LocalSstCache and ReadThroughFile. See local_sst_cache_impl.h for the
// ownership and locking rules, and include/rocksdb/cloud/local_sst_cache.h for
// the behaviour.

#include "cloud/local_sst_cache_impl.h"

#include <algorithm>
#include <chrono>

namespace ROCKSDB_NAMESPACE {

int64_t LocalSstCacheImpl::NowMs() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::steady_clock::now().time_since_epoch())
      .count();
}

std::shared_ptr<LocalSstCache> NewLocalSstCache(
    const LocalSstCacheOptions& opts) {
  return std::make_shared<LocalSstCacheImpl>(opts);
}

LocalSstCacheImpl::LocalSstCacheImpl(const LocalSstCacheOptions& opts)
    : opts_(opts) {
  const int n = std::max(1, opts_.max_concurrent_downloads);
  for (int i = 0; i < n; ++i) {
    workers_.emplace_back([this] { WorkerLoop(); });
  }
  if (opts_.ttl.count() > 0) {
    sweeper_ = std::thread([this] { SweeperLoop(); });
  }
}

LocalSstCacheImpl::~LocalSstCacheImpl() {
  {
    std::lock_guard<std::mutex> lk(mu_);
    stopping_ = true;
  }
  work_cv_.notify_all();
  sweep_cv_.notify_all();
  for (auto& t : workers_) {
    t.join();
  }
  if (sweeper_.joinable()) {
    sweeper_.join();
  }
}

std::shared_ptr<LocalSstCacheImpl::Slot> LocalSstCacheImpl::Acquire(
    const std::string& path, const std::shared_ptr<FileSystem>& base_fs,
    const void* owner) {
  std::lock_guard<std::mutex> lk(mu_);
  auto it = slots_.find(path);
  if (it != slots_.end()) {
    return it->second;
  }
  auto slot = std::make_shared<Slot>(path, base_fs);
  slot->owner = owner;
  slot->last_access_ms.store(NowMs(), std::memory_order_relaxed);
  slots_.emplace(path, slot);
  return slot;
}

void LocalSstCacheImpl::RegisterResident(
    const std::string& path, uint64_t bytes,
    const std::shared_ptr<FileSystem>& base_fs, const void* owner) {
  std::lock_guard<std::mutex> lk(mu_);
  auto it = slots_.find(path);
  std::shared_ptr<Slot> slot;
  if (it == slots_.end()) {
    slot = std::make_shared<Slot>(path, base_fs);
    slots_.emplace(path, slot);
  } else {
    slot = it->second;
  }
  {
    std::lock_guard<std::mutex> sl(slot->mu);
    if (slot->state != State::kRemote) {
      return;  // already accounted (resident, or a download is landing it)
    }
    // Re-check under the lock eviction holds: a caller that saw the file just
    // before an eviction unlinked it must not account a file that is gone.
    if (!slot->base_fs->FileExists(path, IOOptions(), nullptr).ok()) {
      return;
    }
    slot->state = State::kResident;
    slot->bytes = bytes;
    slot->owner = owner;
    slot->removed = false;
  }
  slot->last_access_ms.store(NowMs(), std::memory_order_relaxed);
  resident_bytes_ += bytes;
  ++resident_files_;
  // May evict this very file if nothing else can make room: it is a copy of
  // an object that is already in cloud storage.
  MakeRoomLocked(0, nullptr);
}

LocalSstCacheImpl::Room LocalSstCacheImpl::MakeRoomLocked(
    uint64_t needed, const Slot* candidate) {
  const uint64_t cap = opts_.capacity_bytes;
  if (needed > cap) {
    return Room::kNoSpace;
  }
  uint64_t used = resident_bytes_ + reserved_bytes_;
  if (used + needed <= cap) {
    return Room::kFits;
  }
  uint64_t target = static_cast<uint64_t>(cap * opts_.evict_to_fraction);
  if (target < needed) {
    target = needed;
  }
  std::vector<std::pair<std::shared_ptr<Slot>, uint64_t>> resident;
  resident.reserve(slots_.size());
  for (auto& kv : slots_) {
    std::lock_guard<std::mutex> sl(kv.second->mu);
    if (kv.second->state == State::kResident) {
      resident.emplace_back(kv.second, kv.second->bytes);
    }
  }
  std::sort(resident.begin(), resident.end(),
            [](const auto& a, const auto& b) {
              return a.first->last_access_ms.load(std::memory_order_relaxed) <
                     b.first->last_access_ms.load(std::memory_order_relaxed);
            });
  const bool gated = candidate != nullptr && opts_.frequency_admission;
  const uint32_t cand_hits =
      gated ? candidate->hits.load(std::memory_order_relaxed) : 0;
  auto colder = [&](const Slot& s) {
    return !gated || s.hits.load(std::memory_order_relaxed) < cand_hits;
  };

  // Plan first, evict after: a gated admission that cannot be satisfied must
  // not have evicted anything on the way to finding that out.
  std::vector<std::shared_ptr<Slot>> plan;
  size_t i = 0;
  for (; i < resident.size() && used + needed > cap; ++i) {
    if (!colder(*resident[i].first)) {
      return Room::kColder;
    }
    plan.push_back(resident[i].first);
    used -= std::min(used, resident[i].second);
  }
  if (used + needed > cap) {
    return Room::kNoSpace;  // the rest is downloads in flight
  }
  // Past the minimum, keep going down to the target -- colder files only.
  for (; i < resident.size() && used + needed > target; ++i) {
    if (!colder(*resident[i].first)) {
      break;
    }
    plan.push_back(resident[i].first);
    used -= std::min(used, resident[i].second);
  }
  for (auto& s : plan) {
    EvictSlotLocked(s, /*ttl=*/false);
  }
  return Room::kFits;
}

void LocalSstCacheImpl::MaybeHalveLocked() {
  uint64_t every = opts_.frequency_halving_reads;
  if (every == 0) {
    every = std::max<uint64_t>(1024, 16 * slots_.size());
  }
  if (reads_since_halving_.load(std::memory_order_relaxed) < every) {
    return;
  }
  reads_since_halving_.store(0, std::memory_order_relaxed);
  for (auto& kv : slots_) {
    kv.second->hits.store(kv.second->hits.load(std::memory_order_relaxed) / 2,
                          std::memory_order_relaxed);
  }
}

void LocalSstCacheImpl::EvictSlotLocked(const std::shared_ptr<Slot>& slot,
                                        bool ttl) {
  uint64_t bytes = 0;
  {
    std::lock_guard<std::mutex> sl(slot->mu);
    if (slot->state != State::kResident) {
      return;
    }
    bytes = slot->bytes;
    slot->state = State::kRemote;
    slot->local.reset();  // readers mid-Read keep their own copy
    slot->bytes = 0;
  }
  // Unlink. Space is released when the last open descriptor closes.
  slot->base_fs->DeleteFile(slot->path, IOOptions(), nullptr);
  resident_bytes_ -= std::min(resident_bytes_, bytes);
  if (resident_files_ > 0) {
    --resident_files_;
  }
  ++evictions_;
  evicted_bytes_ += bytes;
  if (ttl) {
    ++ttl_evictions_;
  }
}

bool LocalSstCacheImpl::AdmitGateOpen(Slot& slot) const {
  // A file read 10,000 times while refused must not take the cache mutex, or
  // build a download closure, 10,000 times.
  const int64_t now = NowMs();
  const int64_t last = slot.last_admit_attempt_ms.load(std::memory_order_relaxed);
  if (last != 0 && now - last < opts_.admission_retry_interval.count()) {
    return false;
  }
  slot.last_admit_attempt_ms.store(now, std::memory_order_relaxed);
  return true;
}

bool LocalSstCacheImpl::MaybeAdmit(const std::shared_ptr<Slot>& slot,
                                   uint64_t bytes,
                                   std::function<IOStatus()> download) {
  std::lock_guard<std::mutex> lk(mu_);
  if (stopping_) {
    return false;
  }
  {
    std::lock_guard<std::mutex> sl(slot->mu);
    if (slot->removed || slot->state != State::kRemote) {
      return false;
    }
  }
  if (opts_.capacity_bytes == 0 || bytes > opts_.capacity_bytes) {
    ++refused_space_;
    return false;
  }
  if (in_flight_ >= std::max(1, opts_.max_concurrent_downloads)) {
    ++refused_slots_;
    return false;
  }
  MaybeHalveLocked();
  switch (MakeRoomLocked(bytes, slot.get())) {
    case Room::kFits:
      break;
    case Room::kColder:
      ++refused_colder_;
      return false;
    case Room::kNoSpace:
      ++refused_space_;
      return false;
  }
  {
    std::lock_guard<std::mutex> sl(slot->mu);
    slot->state = State::kDownloading;
    slot->bytes = bytes;
  }
  reserved_bytes_ += bytes;
  ++in_flight_;
  ++downloads_started_;
  queue_.push_back(Task{slot, std::move(download), slot->owner});
  work_cv_.notify_one();
  return true;
}

void LocalSstCacheImpl::WorkerLoop() {
  std::unique_lock<std::mutex> lk(mu_);
  for (;;) {
    work_cv_.wait(lk, [this] { return stopping_ || !queue_.empty(); });
    if (stopping_ && queue_.empty()) {
      return;
    }
    Task task = std::move(queue_.front());
    queue_.pop_front();
    running_owners_.push_back(task.owner);
    lk.unlock();

    IOStatus st = task.download();

    lk.lock();
    auto ro = std::find(running_owners_.begin(), running_owners_.end(),
                        task.owner);
    if (ro != running_owners_.end()) {
      running_owners_.erase(ro);
    }
    --in_flight_;
    const std::shared_ptr<Slot>& slot = task.slot;
    uint64_t bytes = 0;
    bool removed = false;
    {
      std::lock_guard<std::mutex> sl(slot->mu);
      bytes = slot->bytes;
      removed = slot->removed;
      if (!removed && st.ok()) {
        slot->state = State::kResident;
      } else {
        slot->state = State::kRemote;
        slot->bytes = 0;
      }
    }
    reserved_bytes_ -= std::min(reserved_bytes_, bytes);
    if (removed) {
      // RocksDB deleted the file while it was downloading: nothing may keep it.
      if (st.ok()) {
        slot->base_fs->DeleteFile(slot->path, IOOptions(), nullptr);
      }
    } else if (st.ok()) {
      resident_bytes_ += bytes;
      ++resident_files_;
      ++downloads_completed_;
      slot->last_access_ms.store(NowMs(), std::memory_order_relaxed);
    } else {
      ++downloads_failed_;
    }
    done_cv_.notify_all();
  }
}

std::shared_ptr<FSRandomAccessFile> LocalSstCacheImpl::LocalReader(
    Slot& slot) {
  std::lock_guard<std::mutex> sl(slot.mu);
  if (slot.state != State::kResident) {
    return nullptr;
  }
  if (!slot.local) {
    std::unique_ptr<FSRandomAccessFile> f;
    IOStatus st = slot.base_fs->NewRandomAccessFile(slot.path, slot.local_opts,
                                                    &f, nullptr);
    if (!st.ok()) {
      return nullptr;  // read remotely; the next eviction pass cleans up
    }
    slot.local = std::shared_ptr<FSRandomAccessFile>(f.release());
  }
  return slot.local;
}

void LocalSstCacheImpl::Remove(const std::string& path) {
  std::lock_guard<std::mutex> lk(mu_);
  auto it = slots_.find(path);
  if (it == slots_.end()) {
    return;
  }
  std::shared_ptr<Slot> slot = it->second;
  slots_.erase(it);
  std::lock_guard<std::mutex> sl(slot->mu);
  slot->removed = true;
  slot->local.reset();
  if (slot->state == State::kResident) {
    resident_bytes_ -= std::min(resident_bytes_, slot->bytes);
    if (resident_files_ > 0) {
      --resident_files_;
    }
    slot->state = State::kRemote;
    slot->bytes = 0;
  }
  // kDownloading: the worker sees `removed`, releases the reservation and
  // deletes whatever it landed.
}

void LocalSstCacheImpl::ForgetOwner(const void* owner) {
  std::unique_lock<std::mutex> lk(mu_);
  for (auto it = queue_.begin(); it != queue_.end();) {
    if (it->owner != owner) {
      ++it;
      continue;
    }
    {
      std::lock_guard<std::mutex> sl(it->slot->mu);
      reserved_bytes_ -= std::min(reserved_bytes_, it->slot->bytes);
      it->slot->state = State::kRemote;
      it->slot->bytes = 0;
    }
    --in_flight_;
    it = queue_.erase(it);
  }
  done_cv_.wait(lk, [&] {
    return std::find(running_owners_.begin(), running_owners_.end(), owner) ==
           running_owners_.end();
  });
  // Close this owner's local descriptors; the files stay cached on disk and
  // are reopened by the next owner that reads them.
  for (auto& kv : slots_) {
    if (kv.second->owner == owner) {
      std::lock_guard<std::mutex> sl(kv.second->mu);
      kv.second->local.reset();
    }
  }
}

void LocalSstCacheImpl::TtlSweepLocked(int64_t now_ms) {
  const int64_t ttl_ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(opts_.ttl).count();
  if (ttl_ms <= 0) {
    return;
  }
  std::vector<std::shared_ptr<Slot>> expired;
  for (auto& kv : slots_) {
    if (kv.second->state == State::kResident &&
        now_ms - kv.second->last_access_ms.load(std::memory_order_relaxed) >
            ttl_ms) {
      expired.push_back(kv.second);
    }
  }
  for (auto& s : expired) {
    EvictSlotLocked(s, /*ttl=*/true);
  }
}

void LocalSstCacheImpl::SweeperLoop() {
  const auto period = std::max<std::chrono::milliseconds>(
      std::chrono::milliseconds(1000),
      std::chrono::duration_cast<std::chrono::milliseconds>(opts_.ttl) / 4);
  std::unique_lock<std::mutex> lk(mu_);
  while (!stopping_) {
    sweep_cv_.wait_for(lk, period, [this] { return stopping_; });
    if (stopping_) {
      return;
    }
    TtlSweepLocked(NowMs());
  }
}

void LocalSstCacheImpl::EvictNow() {
  std::lock_guard<std::mutex> lk(mu_);
  TtlSweepLocked(NowMs());
  MakeRoomLocked(0, nullptr);
}

LocalSstCacheStats LocalSstCacheImpl::GetStats() const {
  std::lock_guard<std::mutex> lk(mu_);
  LocalSstCacheStats s;
  s.capacity_bytes = opts_.capacity_bytes;
  s.resident_bytes = resident_bytes_;
  s.reserved_bytes = reserved_bytes_;
  s.resident_files = resident_files_;
  s.downloads_started = downloads_started_;
  s.downloads_completed = downloads_completed_;
  s.downloads_failed = downloads_failed_;
  s.admissions_refused_space = refused_space_;
  s.admissions_refused_slots = refused_slots_;
  s.admissions_refused_colder = refused_colder_;
  s.evictions = evictions_;
  s.evicted_bytes = evicted_bytes_;
  s.ttl_evictions = ttl_evictions_;
  s.reads_local = reads_local_.load(std::memory_order_relaxed);
  s.reads_remote = reads_remote_.load(std::memory_order_relaxed);
  return s;
}

// ---------------------------------------------------------------------------

ReadThroughFile::ReadThroughFile(std::shared_ptr<LocalSstCacheImpl> cache,
                                 std::shared_ptr<LocalSstCacheImpl::Slot> slot,
                                 uint64_t file_size,
                                 RemoteFactory remote_factory,
                                 DownloadFactory download_factory)
    : cache_(std::move(cache)),
      slot_(std::move(slot)),
      file_size_(file_size),
      remote_factory_(std::move(remote_factory)),
      download_factory_(std::move(download_factory)) {}

IOStatus ReadThroughFile::Remote(
    std::shared_ptr<FSRandomAccessFile>* out) const {
  std::lock_guard<std::mutex> lk(remote_mu_);
  if (!remote_) {
    std::unique_ptr<CloudStorageReadableFile> f;
    IOStatus st = remote_factory_(&f);
    if (!st.ok()) {
      return st;
    }
    remote_ = std::shared_ptr<FSRandomAccessFile>(f.release());
  }
  *out = remote_;
  return IOStatus::OK();
}

void ReadThroughFile::MaybeAdmit() const {
  if (cache_->AdmitGateOpen(*slot_)) {
    cache_->MaybeAdmit(slot_, file_size_, download_factory_());
  }
}

IOStatus ReadThroughFile::Read(uint64_t offset, size_t n,
                               const IOOptions& options, Slice* result,
                               char* scratch, IODebugContext* dbg) const {
  std::shared_ptr<FSRandomAccessFile> local = cache_->LocalReader(*slot_);
  if (local) {
    cache_->NoteRead(*slot_, true);
    return local->Read(offset, n, options, result, scratch, dbg);
  }
  cache_->NoteRead(*slot_, false);
  MaybeAdmit();
  std::shared_ptr<FSRandomAccessFile> remote;
  IOStatus st = Remote(&remote);
  if (!st.ok()) {
    return st;
  }
  return remote->Read(offset, n, options, result, scratch, dbg);
}

IOStatus ReadThroughFile::Prefetch(uint64_t offset, size_t n,
                                   const IOOptions& options,
                                   IODebugContext* dbg) {
  std::shared_ptr<FSRandomAccessFile> local = cache_->LocalReader(*slot_);
  if (local) {
    return local->Prefetch(offset, n, options, dbg);
  }
  std::shared_ptr<FSRandomAccessFile> remote;
  IOStatus st = Remote(&remote);
  if (!st.ok()) {
    return st;
  }
  return remote->Prefetch(offset, n, options, dbg);
}

}  // namespace ROCKSDB_NAMESPACE
