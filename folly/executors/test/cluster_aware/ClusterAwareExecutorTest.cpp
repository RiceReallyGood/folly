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

#include <atomic>
#include <thread>
#include <vector>

#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/executors/ClusterTopology.h>
#include <folly/executors/task_queue/ClusterAwareBlockingQueue.h>
#include <folly/executors/thread_factory/ClusterAffinityThreadFactory.h>
#include <folly/executors/thread_factory/NamedThreadFactory.h>
#include <gtest/gtest.h>

#include "TestUtil.h"

using namespace folly;
using namespace folly::test;
using namespace std::chrono_literals;

namespace {
using Queue = ClusterAwareBlockingQueue<CPUThreadPoolExecutor::CPUTask>;

std::unique_ptr<CPUThreadPoolExecutor> makeExecutor(
    size_t threads, Queue*& queue) {
  const auto& topo = ClusterTopology::instance();
  auto q = std::make_unique<Queue>(topo);
  queue = q.get();
  return std::make_unique<CPUThreadPoolExecutor>(
      std::make_pair(threads, threads),
      std::move(q),
      std::make_shared<ClusterAffinityThreadFactory>(
          std::make_shared<NamedThreadFactory>("caw"), topo));
}
} // namespace

TEST(ClusterAwareExecutor, RunsAllTasksOnPinnedWorkers) {
  const auto& topo = ClusterTopology::instance();
  Queue* queue = nullptr;
  auto ex = makeExecutor(2 * topo.size(), queue);
  std::atomic<int> done{0};
  std::atomic<int> badHome{0};
  runWithTimeout(
      [&] {
        std::vector<std::thread> producers;
        for (int p = 0; p < 4; ++p) {
          producers.emplace_back([&] {
            for (int n = 0; n < 25000; ++n) {
              ex->add([&] {
                if (currentThreadCluster() < 0) {
                  ++badHome;
                }
                ++done;
              });
            }
          });
        }
        for (auto& t : producers) {
          t.join();
        }
        ex->join();
      },
      60s);
  EXPECT_EQ(done.load(), 100000);
  EXPECT_EQ(badHome.load(), 0);
  EXPECT_GE(queue->stats().localTakes + queue->stats().stolenTakes, 100000u);
}

TEST(ClusterAwareExecutor, JoinDoesNotHangWithRemoteProducer) {
  const auto& topo = ClusterTopology::instance();
  if (topo.size() < 2) {
    GTEST_SKIP() << "needs at least two clusters";
  }
  Queue* queue = nullptr;
  auto ex = makeExecutor(1, queue); // the only worker lives in cluster 0
  std::atomic<int> done{0};
  runWithTimeout(
      [&] {
        std::thread producer([&] {
          setCurrentThreadCluster(int(topo.size()) - 1);
          for (int n = 0; n < 1000; ++n) {
            ex->add([&] { ++done; });
          }
          setCurrentThreadCluster(-1);
        });
        producer.join();
        ex->join();
      },
      10s);
  EXPECT_EQ(done.load(), 1000);
}
