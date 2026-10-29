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

#pragma once

#include <chrono>
#include <cstdlib>
#include <functional>
#include <future>
#include <thread>

#include <folly/executors/ClusterTopology.h>
#include <gtest/gtest.h>

#include "FakeSysfs.h"

namespace folly {
namespace test {

// Runs `fn` and aborts the test binary if it does not finish within
// `timeout` (a hang would otherwise block the whole run).
inline void runWithTimeout(std::function<void()> fn, std::chrono::seconds timeout) {
  auto done = std::async(std::launch::async, std::move(fn));
  if (done.wait_for(timeout) != std::future_status::ready) {
    ADD_FAILURE() << "timed out after " << timeout.count() << "s";
    std::abort();
  }
  done.get();
}

// 2 NUMA nodes x 2 clusters: neighbors(0) = {1, 2, 3}, neighbors(1) = {0, 2, 3},
// neighbors(2) = {3, 0, 1}, neighbors(3) = {2, 0, 1}.
inline const ClusterTopology& fourClusters() {
  static const ClusterTopology topo = ClusterTopology::detect(
      makeFakeSysfs(fourClusterLayout(), fourClusterDistances()), range(0, 15));
  return topo;
}

// Resets the calling thread's home cluster when it goes out of scope.
struct HomeCluster {
  explicit HomeCluster(int c) { setCurrentThreadCluster(c); }
  ~HomeCluster() { setCurrentThreadCluster(-1); }
};

inline bool waitUntil(
    const std::function<bool()>& pred, std::chrono::milliseconds limit) {
  auto deadline = std::chrono::steady_clock::now() + limit;
  while (!pred()) {
    if (std::chrono::steady_clock::now() > deadline) {
      return false;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  return true;
}

} // namespace test
} // namespace folly
