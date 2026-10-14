//  Copyright (c) 2026-present, the rocksdb-cloud maintainers.
//
// MANIFEST deltas: a MANIFEST's growth uploaded as small objects instead of
// the whole file on every sync (CloudFileSystemOptions::upload_manifest_deltas).
//
// Objects next to the base, MANIFEST-<epoch>:
//   mlog-<epoch>-<gen>-base-<size16x>-<hash16x>  marker: the base holds
//                                     generation <gen>'s first <size> bytes,
//                                     whose Hash() is <hash>
//   mlog-<epoch>-<gen>-<off16x>-<end16x>    bytes [off, end) of generation <gen>
// A generation is one MANIFEST writer's lifetime (a new MANIFEST within an
// epoch is a new generation; its id is random). The writer uploads the whole
// file as the base and then its marker, deltas after that, and again the whole
// file (and a new marker) when the deltas outgrow the base.
//
// A reader lists the objects, then takes the base; if a marker of the base's
// size and hash is listed, it appends that generation's deltas in order. The
// base alone is always a complete MANIFEST as of its upload, so without a
// matching marker (a crash between base and marker, an older writer, a whole
// upload that landed after the listing) the base is used as is. The hash keeps
// a marker from matching another generation's base of the same size. A delta
// missing in the middle of the chain is corruption: the reader fails. A delta
// listed but gone when fetched was superseded by a whole upload: the reader
// starts again.
//
// Names never start with "MANIFEST": rocksdb-cloud's invisible-file cleanup
// deletes such objects unless they are the current epoch's MANIFEST.
//
// Header-only and dependency-free so tools outside the library (backups) can
// assemble a MANIFEST the same way.

#pragma once

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <string>
#include <vector>

namespace ROCKSDB_NAMESPACE {
namespace manifest_delta {

constexpr char kPrefix[] = "mlog-";

struct Name {
  enum Kind { kNone, kBase, kDelta } kind = kNone;
  std::string epoch;
  uint64_t gen = 0;
  uint64_t off = 0;   // delta: first byte
  uint64_t end = 0;   // delta: one past the last byte
  uint64_t size = 0;  // marker: base size
  uint64_t hash = 0;  // marker: Hash() of the base
  std::string object; // the name it was parsed from (basename)
};

inline std::string Hex16(uint64_t v) {
  char buf[17];
  snprintf(buf, sizeof(buf), "%016llx", static_cast<unsigned long long>(v));
  return buf;
}

inline std::string Prefix(const std::string& epoch) {
  return std::string(kPrefix) + epoch + "-";
}
inline std::string BaseMarker(const std::string& epoch, uint64_t gen,
                              uint64_t size, uint64_t hash) {
  return Prefix(epoch) + Hex16(gen) + "-base-" + Hex16(size) + "-" + Hex16(hash);
}

// FNV-1a, 64 bit. Pass the previous result as `h` to continue over more bytes.
constexpr uint64_t kHashSeed = 0xcbf29ce484222325ULL;
inline uint64_t Hash(const char* p, size_t n, uint64_t h = kHashSeed) {
  for (size_t i = 0; i < n; ++i) {
    h ^= static_cast<unsigned char>(p[i]);
    h *= 0x100000001b3ULL;
  }
  return h;
}
inline uint64_t Hash(const std::string& s) { return Hash(s.data(), s.size()); }
inline std::string Delta(const std::string& epoch, uint64_t gen, uint64_t off,
                         uint64_t end) {
  return Prefix(epoch) + Hex16(gen) + "-" + Hex16(off) + "-" + Hex16(end);
}

namespace detail {
inline bool ParseHex(const std::string& s, uint64_t* v) {
  if (s.empty() || s.size() > 16) return false;
  uint64_t r = 0;
  for (char c : s) {
    r <<= 4;
    if (c >= '0' && c <= '9') {
      r |= static_cast<uint64_t>(c - '0');
    } else if (c >= 'a' && c <= 'f') {
      r |= static_cast<uint64_t>(c - 'a' + 10);
    } else {
      return false;
    }
  }
  *v = r;
  return true;
}
// Splits off the last `n` '-'-separated fields; the rest stays in *head.
inline bool SplitTail(const std::string& s, size_t n,
                      std::vector<std::string>* tail, std::string* head) {
  std::string rest = s;
  tail->clear();
  for (size_t i = 0; i < n; ++i) {
    const size_t dash = rest.rfind('-');
    if (dash == std::string::npos) return false;
    tail->insert(tail->begin(), rest.substr(dash + 1));
    rest = rest.substr(0, dash);
  }
  *head = rest;
  return true;
}
}  // namespace detail

// Parses an object's basename (any directory part is ignored). Epochs may
// contain '-': the fixed fields are taken from the right.
inline Name Parse(const std::string& object) {
  Name n;
  const size_t slash = object.find_last_of('/');
  const std::string base =
      slash == std::string::npos ? object : object.substr(slash + 1);
  n.object = base;
  if (base.compare(0, sizeof(kPrefix) - 1, kPrefix) != 0) return n;
  const std::string body = base.substr(sizeof(kPrefix) - 1);
  std::vector<std::string> t;
  std::string head;
  // Marker: <epoch>-<gen>-base-<size>-<hash>
  if (detail::SplitTail(body, 4, &t, &head) && t[1] == "base" &&
      detail::ParseHex(t[0], &n.gen) && detail::ParseHex(t[2], &n.size) &&
      detail::ParseHex(t[3], &n.hash) && !head.empty()) {
    n.kind = Name::kBase;
    n.epoch = head;
    return n;
  }
  // Delta: <epoch>-<gen>-<off>-<end>
  if (detail::SplitTail(body, 3, &t, &head) && detail::ParseHex(t[0], &n.gen) &&
      detail::ParseHex(t[1], &n.off) && detail::ParseHex(t[2], &n.end) &&
      n.end > n.off && !head.empty()) {
    n.kind = Name::kDelta;
    n.epoch = head;
    return n;
  }
  return n;
}

// What a reader applies on top of a base of `base_size` bytes whose Hash() is
// `base_hash`, given the object names listed (before the base was read).
struct Plan {
  bool use_deltas = false;  // a marker matches the base
  bool gap = false;         // a delta of the chosen generation is missing
  uint64_t gen = 0;
  std::vector<Name> deltas;  // in order; each starts at or before the end so far
  uint64_t final_size = 0;
};

inline Plan PlanAssembly(uint64_t base_size, uint64_t base_hash,
                         const std::vector<std::string>& names,
                         const std::string& epoch) {
  Plan p;
  p.final_size = base_size;
  std::vector<Name> parsed;
  bool found = false;
  for (const auto& s : names) {
    Name n = Parse(s);
    if (n.kind == Name::kNone || n.epoch != epoch) continue;
    // Generations are time-ordered (NewGeneration): of the markers matching
    // the base, the newest is the writer that uploaded it.
    if (n.kind == Name::kBase && n.size == base_size && n.hash == base_hash &&
        (!found || n.gen > p.gen)) {
      found = true;
      p.gen = n.gen;
    }
    parsed.push_back(n);
  }
  if (!found) return p;
  p.use_deltas = true;
  std::vector<Name> mine;
  for (const auto& d : parsed) {
    if (d.kind == Name::kDelta && d.gen == p.gen) mine.push_back(d);
  }
  std::sort(mine.begin(), mine.end(), [](const Name& a, const Name& b) {
    return a.off < b.off || (a.off == b.off && a.end < b.end);
  });
  uint64_t size = base_size;
  for (const auto& d : mine) {
    if (d.end <= size) continue;  // covered by the base (consolidated)
    if (d.off > size) {
      p.gap = true;  // a delta between `size` and d.off is missing
      break;
    }
    p.deltas.push_back(d);
    size = d.end;
  }
  p.final_size = size;
  return p;
}

// A new generation id: milliseconds since the epoch in the high bits, so a
// later writer's id is larger; random low bits separate writers in one ms.
inline uint64_t NewGeneration(uint64_t now_ms, uint64_t random16) {
  return (now_ms << 16) | (random16 & 0xffff);
}

}  // namespace manifest_delta
}  // namespace ROCKSDB_NAMESPACE
