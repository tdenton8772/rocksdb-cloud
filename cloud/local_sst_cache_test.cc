//  Copyright (c) 2026-present, NAM.  All rights reserved.
//
// Standalone test of LocalSstCache (no S3, no gtest): real files in a temp
// directory, fake downloads. Build against librocksdb.a:
//
//   g++ -std=c++20 -I<fork> -I<fork>/include cloud/local_sst_cache_test.cc \
//       <librocksdb.a> <aws libs...> -lpthread -o local_sst_cache_test
//
// Prints PASS/FAIL per check; exits non-zero on any failure.

#include <unistd.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "cloud/local_sst_cache_impl.h"
#include "rocksdb/file_system.h"

using namespace ROCKSDB_NAMESPACE;

static int g_fail = 0, g_checks = 0;
static void check(bool ok, const std::string& what) {
  ++g_checks;
  std::printf("%s %s\n", ok ? "PASS" : "FAIL", what.c_str());
  if (!ok) ++g_fail;
}

static std::shared_ptr<FileSystem> fs() { return FileSystem::Default(); }
static std::string g_dir;

static std::string path(const std::string& name) { return g_dir + "/" + name; }

static void writeFile(const std::string& p, size_t bytes, char fill = 'x') {
  std::unique_ptr<FSWritableFile> f;
  fs()->NewWritableFile(p, FileOptions(), &f, nullptr);
  std::string buf(bytes, fill);
  f->Append(buf, IOOptions(), nullptr);
  f->Close(IOOptions(), nullptr);
}
static bool exists(const std::string& p) {
  return fs()->FileExists(p, IOOptions(), nullptr).ok();
}
// A fake download: writes the file under a temp name and renames it, the same
// shape as CloudStorageProviderImpl::GetCloudObject.
static std::function<IOStatus()> fakeDownload(const std::string& p,
                                              size_t bytes) {
  return [p, bytes]() -> IOStatus {
    writeFile(p + ".tmp-1", bytes);
    return fs()->RenameFile(p + ".tmp-1", p, IOOptions(), nullptr);
  };
}
static void waitIdle(LocalSstCacheImpl& c) {
  for (int i = 0; i < 500; ++i) {
    auto s = c.GetStats();
    if (s.reserved_bytes == 0) return;
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
}
static LocalSstCacheOptions opts(uint64_t cap, int dl = 2) {
  LocalSstCacheOptions o;
  o.capacity_bytes = cap;
  o.max_concurrent_downloads = dl;
  o.admission_retry_interval = std::chrono::milliseconds(0);
  return o;
}
static void touch(const std::shared_ptr<LocalSstCacheImpl::Slot>& s,
                  int64_t ms) {
  s->last_access_ms.store(ms);
}

// 1. A download lands, is accounted resident, and becomes readable locally.
static void testAdmitAndLand() {
  auto c = std::make_shared<LocalSstCacheImpl>(opts(1000));
  auto s = c->Acquire(path("a.sst"), fs(), nullptr);
  check(c->LocalReader(*s) == nullptr, "1 not local before download");
  check(c->MaybeAdmit(s, 400, fakeDownload(path("a.sst"), 400)),
        "1 admission accepted within budget");
  waitIdle(*c);
  auto st = c->GetStats();
  check(st.resident_bytes == 400 && st.resident_files == 1 &&
            st.downloads_completed == 1,
        "1 download landed and is accounted resident");
  auto r = c->LocalReader(*s);
  check(r != nullptr, "1 local reader available after landing");
  char scratch[16];
  Slice out;
  check(r && r->Read(0, 8, IOOptions(), &out, scratch, nullptr).ok() &&
            out.size() == 8,
        "1 local read succeeds");
}

// 2. A file larger than the budget is never downloaded.
static void testTooBig() {
  auto c = std::make_shared<LocalSstCacheImpl>(opts(1000));
  auto s = c->Acquire(path("big.sst"), fs(), nullptr);
  check(!c->MaybeAdmit(s, 1500, fakeDownload(path("big.sst"), 1500)),
        "2 file larger than the budget is refused");
  check(c->GetStats().admissions_refused_space == 1 && !exists(path("big.sst")),
        "2 refusal counted and nothing written");
}

// 3. LRU: admitting a new file evicts the least recently used first.
static void testLru() {
  auto o = opts(1000);
  o.evict_to_fraction = 1.0;
  auto c = std::make_shared<LocalSstCacheImpl>(o);
  const char* names[] = {"l1.sst", "l2.sst", "l3.sst"};
  std::shared_ptr<LocalSstCacheImpl::Slot> slots[3];
  for (int i = 0; i < 3; ++i) {
    writeFile(path(names[i]), 300);
    c->RegisterResident(path(names[i]), 300, fs(), nullptr);
    slots[i] = c->Acquire(path(names[i]), fs(), nullptr);
  }
  // Access order: l2 oldest, then l1, then l3 newest.
  touch(slots[1], 1000);
  touch(slots[0], 2000);
  touch(slots[2], 3000);
  auto n = c->Acquire(path("l4.sst"), fs(), nullptr);
  n->hits.store(1);  // read more often than the residents (0)
  check(c->MaybeAdmit(n, 300, fakeDownload(path("l4.sst"), 300)),
        "3 admission accepted after eviction");
  waitIdle(*c);
  check(!exists(path("l2.sst")) && exists(path("l1.sst")) &&
            exists(path("l3.sst")) && exists(path("l4.sst")),
        "3 least recently used file (l2) evicted, others kept");
  auto st = c->GetStats();
  check(st.resident_bytes == 900 && st.evictions == 1,
        "3 accounting: 900 resident, 1 eviction");
}

// 4. The budget holds on registration too: a written file over budget evicts.
static void testRegisterOverBudget() {
  auto o = opts(1000);
  o.evict_to_fraction = 1.0;
  auto c = std::make_shared<LocalSstCacheImpl>(o);
  for (int i = 0; i < 5; ++i) {
    std::string p = path("w" + std::to_string(i) + ".sst");
    writeFile(p, 300);
    c->RegisterResident(p, 300, fs(), nullptr);
    auto s = c->Acquire(p, fs(), nullptr);
    touch(s, 1000 + i);
  }
  auto st = c->GetStats();
  check(st.resident_bytes <= 1000, "4 registered files never exceed the budget (" +
                                       std::to_string(st.resident_bytes) + ")");
}

// 5. Evicting a file that is open: the held reader keeps working, the slot
//    goes back to remote, the file is unlinked.
static void testEvictOpen() {
  auto o = opts(1000);
  o.evict_to_fraction = 1.0;
  auto c = std::make_shared<LocalSstCacheImpl>(o);
  writeFile(path("open.sst"), 600, 'q');
  c->RegisterResident(path("open.sst"), 600, fs(), nullptr);
  auto s = c->Acquire(path("open.sst"), fs(), nullptr);
  touch(s, 1);
  auto held = c->LocalReader(*s);
  check(held != nullptr, "5 open reader obtained");
  // Force eviction by admitting something that needs the space.
  auto n = c->Acquire(path("new.sst"), fs(), nullptr);
  n->hits.store(1);
  check(c->MaybeAdmit(n, 600, fakeDownload(path("new.sst"), 600)),
        "5 admission evicts the open file");
  waitIdle(*c);
  check(!exists(path("open.sst")), "5 evicted file unlinked");
  check(c->LocalReader(*s) == nullptr, "5 slot falls back to remote");
  char scratch[8];
  Slice out;
  check(held->Read(0, 4, IOOptions(), &out, scratch, nullptr).ok() &&
            out.ToString() == "qqqq",
        "5 reader held across eviction still reads the old data");
}

// 6. The concurrent-download cap refuses rather than queues without bound.
static void testDownloadCap() {
  auto c = std::make_shared<LocalSstCacheImpl>(opts(10000, /*dl=*/1));
  std::mutex m;
  std::condition_variable cv;
  bool release = false;
  auto blocking = [&]() -> IOStatus {
    std::unique_lock<std::mutex> lk(m);
    cv.wait(lk, [&] { return release; });
    writeFile(path("b1.sst"), 100);
    return IOStatus::OK();
  };
  auto s1 = c->Acquire(path("b1.sst"), fs(), nullptr);
  auto s2 = c->Acquire(path("b2.sst"), fs(), nullptr);
  check(c->MaybeAdmit(s1, 100, blocking), "6 first download starts");
  check(!c->MaybeAdmit(s2, 100, fakeDownload(path("b2.sst"), 100)),
        "6 second download refused at the cap");
  check(c->GetStats().admissions_refused_slots == 1, "6 slot refusal counted");
  {
    std::lock_guard<std::mutex> lk(m);
    release = true;
  }
  cv.notify_all();
  waitIdle(*c);
  check(c->MaybeAdmit(s2, 100, fakeDownload(path("b2.sst"), 100)),
        "6 second download starts once a slot frees");
  waitIdle(*c);
}

// 7. RocksDB deletes a file while it downloads: nothing is kept or counted.
static void testRemoveWhileDownloading() {
  auto c = std::make_shared<LocalSstCacheImpl>(opts(10000, 1));
  std::mutex m;
  std::condition_variable cv;
  bool release = false;
  const std::string p = path("gone.sst");
  auto blocking = [&, p]() -> IOStatus {
    std::unique_lock<std::mutex> lk(m);
    cv.wait(lk, [&] { return release; });
    writeFile(p, 200);
    return IOStatus::OK();
  };
  auto s = c->Acquire(p, fs(), nullptr);
  check(c->MaybeAdmit(s, 200, blocking), "7 download starts");
  c->Remove(p);
  {
    std::lock_guard<std::mutex> lk(m);
    release = true;
  }
  cv.notify_all();
  waitIdle(*c);
  auto st = c->GetStats();
  check(!exists(p) && st.resident_bytes == 0 && st.reserved_bytes == 0,
        "7 removed-while-downloading file deleted, nothing accounted");
}

// 8. TTL evicts a file nobody has read for longer than the TTL.
static void testTtl() {
  auto o = opts(10000);
  o.ttl = std::chrono::seconds(1);
  auto c = std::make_shared<LocalSstCacheImpl>(o);
  writeFile(path("old.sst"), 100);
  c->RegisterResident(path("old.sst"), 100, fs(), nullptr);
  auto s = c->Acquire(path("old.sst"), fs(), nullptr);
  touch(s, LocalSstCacheImpl::NowMs() - 5000);
  c->EvictNow();
  auto st = c->GetStats();
  check(!exists(path("old.sst")) && st.ttl_evictions == 1 &&
            st.resident_bytes == 0,
        "8 expired file evicted by TTL");
}

// 9. ForgetOwner waits for that owner's running download before returning.
static void testForgetOwnerWaits() {
  auto c = std::make_shared<LocalSstCacheImpl>(opts(10000, 1));
  int owner_token;
  const void* owner = &owner_token;
  std::atomic<bool> finished{false};
  auto slow = [&]() -> IOStatus {
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    writeFile(path("slow.sst"), 50);
    finished = true;
    return IOStatus::OK();
  };
  auto s = c->Acquire(path("slow.sst"), fs(), owner);
  check(c->MaybeAdmit(s, 50, slow), "9 download starts");
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  c->ForgetOwner(owner);
  check(finished.load(), "9 ForgetOwner returned only after the download ran");
}

// 10. Frequency gate: a file read no more often than the residents it would
//     evict is refused and evicts nothing; once it is read more, it gets in
//     and only the colder file goes.
static void testFrequencyGate() {
  auto o = opts(1000);
  o.evict_to_fraction = 1.0;
  auto c = std::make_shared<LocalSstCacheImpl>(o);
  std::shared_ptr<LocalSstCacheImpl::Slot> r[3];
  for (int i = 0; i < 3; ++i) {
    std::string p = path("f" + std::to_string(i) + ".sst");
    writeFile(p, 300);
    c->RegisterResident(p, 300, fs(), nullptr);
    r[i] = c->Acquire(p, fs(), nullptr);
    touch(r[i], 1000 + i);  // f0 least recently used
  }
  r[0]->hits.store(3);
  r[1]->hits.store(5);
  r[2]->hits.store(5);
  auto n = c->Acquire(path("fn.sst"), fs(), nullptr);
  n->hits.store(3);  // equal to the LRU victim: not hotter
  check(!c->MaybeAdmit(n, 300, fakeDownload(path("fn.sst"), 300)),
        "10 new file no hotter than its victim is refused");
  auto st = c->GetStats();
  check(st.admissions_refused_colder == 1 && st.evictions == 0 &&
            exists(path("f0.sst")) && st.resident_bytes == 900,
        "10 refusal counted, nothing evicted");
  n->hits.store(4);
  check(c->MaybeAdmit(n, 300, fakeDownload(path("fn.sst"), 300)),
        "10 hotter new file admitted");
  waitIdle(*c);
  check(!exists(path("f0.sst")) && exists(path("f1.sst")) &&
            exists(path("f2.sst")) && exists(path("fn.sst")),
        "10 only the colder LRU file (f0) evicted");
}

// 11. A gated admission that needs two victims, where the second is hotter,
//     evicts neither.
static void testFrequencyGateAllOrNothing() {
  auto o = opts(1000);
  o.evict_to_fraction = 1.0;
  auto c = std::make_shared<LocalSstCacheImpl>(o);
  std::shared_ptr<LocalSstCacheImpl::Slot> r[2];
  for (int i = 0; i < 2; ++i) {
    std::string p = path("g" + std::to_string(i) + ".sst");
    writeFile(p, 450);
    c->RegisterResident(p, 450, fs(), nullptr);
    r[i] = c->Acquire(p, fs(), nullptr);
    touch(r[i], 1000 + i);
  }
  r[0]->hits.store(1);
  r[1]->hits.store(9);
  auto n = c->Acquire(path("gn.sst"), fs(), nullptr);
  n->hits.store(5);  // beats g0, not g1; needs both gone to fit 900
  check(!c->MaybeAdmit(n, 900, fakeDownload(path("gn.sst"), 900)),
        "11 admission needing a hotter victim is refused");
  check(exists(path("g0.sst")) && exists(path("g1.sst")) &&
            c->GetStats().evictions == 0,
        "11 the colder victim was not evicted either");
}

// 12. Free space: the first read admits, whatever the counts; and a uniform
//     sweep over twice the budget does not churn.
static void testFreeSpaceAndSweep() {
  auto o = opts(1000, /*dl=*/8);
  auto c = std::make_shared<LocalSstCacheImpl>(o);
  auto a = c->Acquire(path("h0.sst"), fs(), nullptr);
  check(c->MaybeAdmit(a, 100, fakeDownload(path("h0.sst"), 100)),
        "12 first read admitted while there is free space (hits 0)");
  waitIdle(*c);
  // 20 files of 100 bytes against a 1000-byte budget, read round-robin.
  std::vector<std::shared_ptr<LocalSstCacheImpl::Slot>> s;
  for (int i = 1; i <= 20; ++i) {
    s.push_back(c->Acquire(path("h" + std::to_string(i) + ".sst"), fs(), nullptr));
  }
  for (int pass = 0; pass < 10; ++pass) {
    for (size_t i = 0; i < s.size(); ++i) {
      bool local = c->LocalReader(*s[i]) != nullptr;
      c->NoteRead(*s[i], local);
      if (!local) {
        std::string p = path("h" + std::to_string(i + 1) + ".sst");
        c->MaybeAdmit(s[i], 100, fakeDownload(p, 100));
      }
      waitIdle(*c);
    }
  }
  auto st = c->GetStats();
  check(st.resident_bytes <= 1000, "12 sweep stays within budget");
  check(st.downloads_started <= 30,
        "12 uniform sweep over 2x the budget does not churn (" +
            std::to_string(st.downloads_started) + " downloads for 21 files, " +
            std::to_string(st.evictions) + " evictions)");
  check(st.reads_local > 0, "12 the resident part is served locally");
}

int main() {
  char tmpl[] = "/tmp/lsc_test.XXXXXX";
  g_dir = mkdtemp(tmpl);
  testAdmitAndLand();
  testTooBig();
  testLru();
  testRegisterOverBudget();
  testEvictOpen();
  testDownloadCap();
  testRemoveWhileDownloading();
  testTtl();
  testForgetOwnerWaits();
  testFrequencyGate();
  testFrequencyGateAllOrNothing();
  testFreeSpaceAndSweep();
  std::string rm = "rm -rf '" + g_dir + "'";
  if (std::system(rm.c_str()) != 0) {
  }
  std::printf("\n%d checks, %d failures\n", g_checks, g_fail);
  return g_fail ? 1 : 0;
}
