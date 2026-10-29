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

// LLCAccessSpreader, ported from facebook/folly 41068ce38
// folly/concurrency/CacheLocality.{h,cpp} into folly::striped_port. Changes
// from upstream:
// - The base CacheLocality has no per-CPU cache equivalence classes, so the
//   last-level cache of each CPU is read from sysfs here: the highest
//   cache/indexN that is not an instruction cache, keyed by the lowest CPU in
//   its shared_cpu_list (the same grouping upstream takes from
//   CacheLocality::readFromSysfs()).
// - Without a vDSO getcpu (arm64 has none), upstream falls back to
//   FallbackGetcpuType, which returns a per-thread sequence number instead of
//   the CPU, so stripes follow thread creation order rather than the LLC the
//   thread runs on. Here the fallback is sched_getcpu() (glibc reads it from
//   rseq), so that stripes follow the LLC as upstream intends.

#pragma once

#include <cstddef>
#include <vector>

#include <folly/concurrency/CacheLocality.h>

namespace folly {
namespace striped_port {

/// Similar to AccessSpreader, but it has exactly one stripe for each last-level
/// cache that is accessible by the current process.
///
/// Only supported on Linux; on other systems, numStripes() always returns 1
/// and current() always returns 0.
class LLCAccessSpreader {
  struct PrivateTag {};

 public:
  static LLCAccessSpreader& get();

  explicit LLCAccessSpreader(PrivateTag);

  size_t current() const;
  size_t numStripes() const;

 private:
  Getcpu::Func getcpu_;
  size_t numStripes_;
  std::vector<size_t> stripeByCpu_;
};

} // namespace striped_port
} // namespace folly
