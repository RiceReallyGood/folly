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

#include <folly/executors/ClusterTopology.h>

#include <sched.h>

#include <iostream>
#include <stdexcept>

#include <gtest/gtest.h>

#include "FakeSysfs.h"

using namespace folly;
using namespace folly::test;

namespace {
ClusterTopology fourClusters(std::vector<int> allowed = range(0, 15)) {
  return ClusterTopology::detect(
      makeFakeSysfs(fourClusterLayout(), fourClusterDistances()),
      std::move(allowed));
}
} // namespace

TEST(ClusterTopology, GroupsByClusterId) {
  auto t = fourClusters();
  ASSERT_EQ(t.size(), 4);
  EXPECT_EQ(t.clusters()[0].cpus, range(0, 3));
  EXPECT_EQ(t.clusters()[3].cpus, range(12, 15));
  EXPECT_EQ(t.clusters()[1].numaNode, 0);
  EXPECT_EQ(t.clusters()[2].numaNode, 1);
  EXPECT_EQ(t.clusterOfCpu(5), 1);
}

TEST(ClusterTopology, NeighborsSameNumaFirst) {
  auto t = fourClusters();
  EXPECT_EQ(t.clusters()[0].neighbors, (std::vector<int>{1, 2, 3}));
  EXPECT_EQ(t.clusters()[2].neighbors, (std::vector<int>{3, 0, 1}));
}

TEST(ClusterTopology, PartialCpusetKeepsPartialClusters) {
  auto t = fourClusters({2, 3, 4, 5, 6, 7, 8});
  ASSERT_EQ(t.size(), 3);
  EXPECT_EQ(t.clusters()[0].cpus, (std::vector<int>{2, 3}));
  EXPECT_EQ(t.clusters()[2].cpus, (std::vector<int>{8}));
  EXPECT_EQ(t.clusterOfCpu(0), -1);
}

TEST(ClusterTopology, FallsBackToL3WhenNoClusterId) {
  auto t = ClusterTopology::detect(
      makeFakeSysfs(
          fourClusterLayout(),
          fourClusterDistances(),
          /*withClusterId=*/false),
      range(0, 15));
  ASSERT_EQ(t.size(), 4);
  EXPECT_EQ(t.clusters()[0].cpus, range(0, 3));
  EXPECT_EQ(t.clusters()[3].cpus, range(12, 15));
  EXPECT_EQ(t.clusters()[2].numaNode, 1);
}

TEST(ClusterTopology, SingleGroupWhenSysfsMissing) {
  auto t = ClusterTopology::detect("/nonexistent", range(0, 7));
  ASSERT_EQ(t.size(), 1);
  EXPECT_EQ(t.clusters()[0].cpus, range(0, 7));
  EXPECT_TRUE(t.clusters()[0].neighbors.empty());
}

TEST(ClusterTopology, ParseCpuList) {
  EXPECT_EQ(
      parseCpuList("0-3,8,10-11"), (std::vector<int>{0, 1, 2, 3, 8, 10, 11}));
  EXPECT_TRUE(parseCpuList("").empty());
  EXPECT_THROW(parseCpuList("3-1"), std::invalid_argument);
  EXPECT_THROW(parseCpuList("a"), std::invalid_argument);
}

TEST(ClusterTopology, CallerClusterUsesThreadLocalThenFallsBackToZero) {
  auto t = fourClusters();
  setCurrentThreadCluster(2);
  EXPECT_EQ(callerCluster(t), 2);
  setCurrentThreadCluster(7);
  int c = callerCluster(t);
  EXPECT_GE(c, 0);
  EXPECT_LT(c, 4);
  setCurrentThreadCluster(-1);
  EXPECT_EQ(callerCluster(ClusterTopology::detect("/nonexistent", {100000})), 0);
}

TEST(ClusterTopology, DetectsRealMachine) {
  const auto& t = ClusterTopology::instance();
  cpu_set_t set;
  CPU_ZERO(&set);
  ASSERT_EQ(sched_getaffinity(0, sizeof(set), &set), 0);
  size_t total = 0;
  for (const auto& c : t.clusters()) {
    total += c.cpus.size();
  }
  EXPECT_EQ(total, size_t(CPU_COUNT(&set)));
  EXPECT_GE(t.size(), 1);
  std::cout << t.describe();
}
