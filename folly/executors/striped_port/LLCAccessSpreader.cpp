/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Ported from facebook/folly 41068ce38 folly/concurrency/CacheLocality.cpp;
// see LLCAccessSpreader.h for the changes.

#include <folly/executors/striped_port/LLCAccessSpreader.h>

#include <string>
#include <unordered_map>

#include <glog/logging.h>

#include <folly/FileUtil.h>
#include <folly/Indestructible.h>
#include <folly/String.h>
#include <folly/executors/ClusterTopology.h>

#ifdef __linux__
#include <sched.h>
#endif

namespace folly {
namespace striped_port {

namespace {

#ifdef __linux__
int schedGetcpu(unsigned* cpu, unsigned* node, void* /* unused */) {
  int c = sched_getcpu();
  if (c < 0) {
    return -1;
  }
  if (cpu) {
    *cpu = static_cast<unsigned>(c);
  }
  if (node) {
    *node = 0; // unused by LLCAccessSpreader
  }
  return 0;
}

// Lowest CPU that shares the last-level cache of `cpu`, or -1 when sysfs has
// no cache information.
int lastLevelCacheKey(int cpu) {
  const std::string base =
      "/sys/devices/system/cpu/cpu" + std::to_string(cpu) + "/cache/index";
  int key = -1;
  for (int index = 0;; ++index) {
    const std::string dir = base + std::to_string(index) + "/";
    std::string list;
    if (!readFile((dir + "shared_cpu_list").c_str(), list)) {
      break;
    }
    std::string type;
    if (readFile((dir + "type").c_str(), type) &&
        rtrimWhitespace(type) == "Instruction") {
      continue;
    }
    auto cpus = parseCpuList(rtrimWhitespace(list));
    if (!cpus.empty()) {
      key = cpus.front();
    }
  }
  return key;
}
#endif // __linux__

} // namespace

/* static */ LLCAccessSpreader& LLCAccessSpreader::get() {
  static folly::Indestructible<LLCAccessSpreader> instance(PrivateTag{});
  return *instance;
}

LLCAccessSpreader::LLCAccessSpreader(PrivateTag) {
#ifdef __linux__
  auto getcpu = Getcpu::resolveVdsoFunc();
  getcpu_ = getcpu ? getcpu : &schedGetcpu;

  // Only have stripes for LLCs that are accessible from the current process,
  // and number them sequentially as we discover them.
  std::unordered_map<int, size_t> cacheIdx;
  cpu_set_t cpuset;
  PCHECK(sched_getaffinity(0, sizeof(cpuset), &cpuset) == 0);
  for (int cpu = 0; cpu < CPU_SETSIZE; ++cpu) {
    if (!CPU_ISSET(cpu, &cpuset)) {
      continue;
    }
    auto [it, _] =
        cacheIdx.try_emplace(lastLevelCacheKey(cpu), cacheIdx.size());
    if (stripeByCpu_.size() <= static_cast<size_t>(cpu)) {
      stripeByCpu_.resize(cpu + 1, 0);
    }
    stripeByCpu_[cpu] = it->second;
  }

  numStripes_ = std::max<size_t>(cacheIdx.size(), 1);
#else
  numStripes_ = 1;
  (void)getcpu_;
  (void)stripeByCpu_;
#endif // __linux__
}

size_t LLCAccessSpreader::current() const {
#ifdef __linux__
  struct ThreadCache {
    size_t usesLeft = 0;
    size_t value;
  };

  // We expect that a thread doesn't migrate LLC often, so reuse the value a
  // few times before refreshing it.
  thread_local ThreadCache tc;
  if (tc.usesLeft-- == 0) {
    tc.usesLeft = 16; // A small number is enough to amortize.
    unsigned cpu;
    getcpu_(&cpu, nullptr, nullptr);
    // If the set of active CPUs can change at runtime just return the 0
    // stripe. This should not happen in normal operations.
    tc.value = cpu < stripeByCpu_.size() ? stripeByCpu_[cpu] : 0;
  }

  return tc.value;
#else
  return 0;
#endif // __linux__
}

size_t LLCAccessSpreader::numStripes() const {
  return numStripes_;
}

} // namespace striped_port
} // namespace folly
