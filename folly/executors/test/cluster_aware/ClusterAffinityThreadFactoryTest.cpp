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

#include <folly/executors/thread_factory/ClusterAffinityThreadFactory.h>

#include <sched.h>

#include <algorithm>
#include <stdexcept>

#include <folly/executors/ClusterTopology.h>
#include <folly/executors/thread_factory/NamedThreadFactory.h>
#include <gtest/gtest.h>

using namespace folly;

namespace {
std::vector<int> currentAffinity() {
  cpu_set_t set;
  CPU_ZERO(&set);
  EXPECT_EQ(sched_getaffinity(0, sizeof(set), &set), 0);
  std::vector<int> cpus;
  for (int cpu = 0; cpu < CPU_SETSIZE; ++cpu) {
    if (CPU_ISSET(cpu, &set)) {
      cpus.push_back(cpu);
    }
  }
  return cpus;
}

std::shared_ptr<ThreadFactory> inner() {
  return std::make_shared<NamedThreadFactory>("t");
}
} // namespace

TEST(ClusterAffinityThreadFactory, RoundRobinPinsToClusterCpus) {
  const auto& topo = ClusterTopology::instance();
  ClusterAffinityThreadFactory f(inner(), topo);
  size_t n = 2 * topo.size();
  std::vector<std::vector<int>> masks(n);
  std::vector<int> homes(n, -2);
  for (size_t i = 0; i < n; ++i) {
    f.newThread([&, i] {
       masks[i] = currentAffinity();
       homes[i] = currentThreadCluster();
     }).join();
  }
  for (size_t i = 0; i < n; ++i) {
    EXPECT_EQ(homes[i], int(i % topo.size()));
    EXPECT_EQ(masks[i], topo.clusters()[i % topo.size()].cpus);
  }
}

TEST(ClusterAffinityThreadFactory, FixedCpusetMode) {
  const auto& topo = ClusterTopology::instance();
  const auto& last = topo.clusters().back();
  std::vector<int> cpus(
      last.cpus.begin(),
      last.cpus.begin() + std::min<size_t>(2, last.cpus.size()));
  ClusterAffinityThreadFactory f(inner(), topo, cpus);
  std::vector<int> mask;
  int home = -2;
  f.newThread([&] {
     mask = currentAffinity();
     home = currentThreadCluster();
   }).join();
  EXPECT_EQ(mask, cpus);
  EXPECT_EQ(home, int(topo.size()) - 1);
}

TEST(ClusterAffinityThreadFactory, FixedCpusetRejectsEmptyOrUnknown) {
  const auto& topo = ClusterTopology::instance();
  EXPECT_THROW(
      ClusterAffinityThreadFactory(inner(), topo, std::vector<int>{}),
      std::invalid_argument);
  EXPECT_THROW(
      ClusterAffinityThreadFactory(inner(), topo, std::vector<int>{100000}),
      std::invalid_argument);
}
