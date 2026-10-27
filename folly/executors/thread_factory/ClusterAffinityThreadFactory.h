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

#include <atomic>
#include <memory>
#include <thread>
#include <vector>

#include <folly/executors/thread_factory/ThreadFactory.h>

namespace folly {

class ClusterTopology;

// Pins each thread it creates to one CPU cluster (all of that cluster's
// allowed CPUs, so the kernel can still balance inside the cluster) and
// records the cluster as the thread's home cluster (currentThreadCluster()).
class ClusterAffinityThreadFactory : public ThreadFactory {
 public:
  // Round-robin: the i-th thread created goes to cluster i % topo.size().
  ClusterAffinityThreadFactory(
      std::shared_ptr<ThreadFactory> inner, const ClusterTopology& topo);

  // Fixed: every thread is pinned to `cpus`, and its home cluster is the one
  // containing cpus[0]. Throws std::invalid_argument if `cpus` is empty or
  // contains a CPU that is not in `topo`.
  ClusterAffinityThreadFactory(
      std::shared_ptr<ThreadFactory> inner,
      const ClusterTopology& topo,
      std::vector<int> cpus);

  std::thread newThread(Func&& func) override;

 private:
  std::shared_ptr<ThreadFactory> inner_;
  const ClusterTopology& topo_;
  std::vector<int> fixedCpus_;
  int fixedCluster_{-1};
  std::atomic<size_t> next_{0};
};

} // namespace folly
