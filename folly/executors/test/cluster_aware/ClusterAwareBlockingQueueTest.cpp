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

#include <folly/executors/task_queue/ClusterAwareBlockingQueue.h>

#include <atomic>
#include <chrono>
#include <future>
#include <thread>
#include <vector>

#include <folly/executors/ClusterTopology.h>
#include <gtest/gtest.h>

#include "TestUtil.h"

using namespace folly;
using namespace folly::test;
using namespace std::chrono_literals;

TEST(ClusterAwareBlockingQueue, LocalFirstThenNearestSteal) {
  ClusterAwareBlockingQueue<int> q(fourClusters());
  q.addToCluster(3, 30);
  q.addToCluster(0, 0);
  q.addToCluster(1, 10);
  q.addToCluster(1, 11);
  HomeCluster home(1);
  EXPECT_EQ(q.take(), 10);
  EXPECT_EQ(q.take(), 11);
  EXPECT_EQ(q.take(), 0);
  EXPECT_EQ(q.take(), 30);
  EXPECT_EQ(q.size(), 0);
  EXPECT_EQ(q.stats().localTakes, 2);
  EXPECT_EQ(q.stats().stolenTakes, 2);
}

// CPUThreadPoolExecutor::join() enqueues its stop tasks with LO_PRI and
// relies on them running after every queued task.
TEST(ClusterAwareBlockingQueue, LowPriorityItemsRunAfterAllClusterQueues) {
  ClusterAwareBlockingQueue<int> q(fourClusters());
  q.addToCluster(3, 1);
  q.addToCluster(1, 2);
  HomeCluster home(0);
  q.addWithPriority(99, Executor::LO_PRI);
  EXPECT_EQ(q.take(), 2);
  EXPECT_EQ(q.take(), 1);
  EXPECT_EQ(q.take(), 99);
  EXPECT_EQ(q.size(), 0);
}

TEST(ClusterAwareBlockingQueue, AddUsesCallerCluster) {
  ClusterAwareBlockingQueue<int> q(fourClusters());
  HomeCluster home(2);
  q.add(5);
  EXPECT_EQ(q.sizeOfCluster(2), 1);
  EXPECT_EQ(q.size(), 1);
}

TEST(ClusterAwareBlockingQueue, TryTakeForTimesOut) {
  ClusterAwareBlockingQueue<int> q(fourClusters());
  HomeCluster home(0);
  auto start = std::chrono::steady_clock::now();
  EXPECT_FALSE(q.try_take_for(20ms).has_value());
  EXPECT_GE(std::chrono::steady_clock::now() - start, 20ms);
  EXPECT_EQ(q.idleWorkers(), 0);
}

TEST(ClusterAwareBlockingQueue, IdleRemoteWorkerIsWokenWhenLocalHasNone) {
  ClusterAwareBlockingQueue<int> q(fourClusters());
  std::promise<int> got;
  std::thread worker([&] {
    HomeCluster home(2);
    got.set_value(q.take());
  });
  ASSERT_TRUE(waitUntil([&] { return q.idleWorkers() == 1; }, 1000ms));
  q.addToCluster(0, 42); // cluster 0 has no worker
  auto f = got.get_future();
  ASSERT_EQ(f.wait_for(1s), std::future_status::ready);
  EXPECT_EQ(f.get(), 42);
  worker.join();
}

// Back-to-back items must wake distinct idle workers, even when they sit in
// different clusters: waking the same cluster twice would leave the second
// item waiting for the first one to finish (or forever, if that worker exits).
TEST(ClusterAwareBlockingQueue, ConsecutiveItemsWakeDistinctIdleWorkers) {
  ClusterAwareBlockingQueue<int> q(fourClusters());
  std::promise<int> got1;
  std::promise<int> got2;
  std::thread w1([&] {
    HomeCluster home(1);
    got1.set_value(q.take());
  });
  std::thread w2([&] {
    HomeCluster home(2);
    got2.set_value(q.take());
  });
  ASSERT_TRUE(waitUntil([&] { return q.idleWorkers() == 2; }, 1000ms));
  q.addToCluster(0, 1);
  q.addToCluster(0, 2);
  auto f1 = got1.get_future();
  auto f2 = got2.get_future();
  bool both = f1.wait_for(1s) == std::future_status::ready &&
      f2.wait_for(1s) == std::future_status::ready;
  EXPECT_TRUE(both);
  if (!both) {
    q.addToCluster(1, -1); // unblock whoever is still waiting
    q.addToCluster(2, -1);
  }
  w1.join();
  w2.join();
}

TEST(ClusterAwareBlockingQueue, BusyRemoteWorkerPicksUpTaskFromWorkerlessCluster) {
  ClusterAwareBlockingQueue<int> q(fourClusters());
  std::promise<int> second;
  std::atomic<bool> busy{false};
  std::thread worker([&] {
    HomeCluster home(2);
    EXPECT_EQ(q.take(), 1);
    busy = true;
    std::this_thread::sleep_for(50ms);
    second.set_value(q.take());
  });
  q.addToCluster(2, 1);
  ASSERT_TRUE(waitUntil([&] { return busy.load(); }, 1000ms));
  q.addToCluster(0, 2); // no idle worker anywhere right now
  auto f = second.get_future();
  ASSERT_EQ(f.wait_for(1s), std::future_status::ready);
  EXPECT_EQ(f.get(), 2);
  worker.join();
}

TEST(ClusterAwareBlockingQueue, StressNoLostTasks) {
  constexpr int kProducers = 8;
  constexpr int kWorkers = 16;
  constexpr int kPerProducer = 200000;
  ClusterAwareBlockingQueue<int> q(fourClusters());
  std::atomic<long> total{0};
  runWithTimeout(
      [&] {
        std::vector<std::thread> workers;
        for (int i = 0; i < kWorkers; ++i) {
          workers.emplace_back([&, i] {
            HomeCluster home(i % 4);
            long sum = 0;
            while (true) {
              int v;
              if (i % 2 == 0) {
                v = q.take();
              } else {
                auto item = q.try_take_for(1ms);
                if (!item) {
                  continue;
                }
                v = *item;
              }
              if (v < 0) {
                break;
              }
              sum += v;
            }
            total += sum;
          });
        }
        std::vector<std::thread> producers;
        for (int p = 0; p < kProducers; ++p) {
          producers.emplace_back([&, p] {
            HomeCluster home(p % 4);
            for (int n = 0; n < kPerProducer; ++n) {
              q.add(1);
            }
          });
        }
        for (auto& t : producers) {
          t.join();
        }
        for (int i = 0; i < kWorkers; ++i) {
          q.addToCluster(0, -1);
        }
        for (auto& t : workers) {
          t.join();
        }
      },
      60s);
  EXPECT_EQ(total.load(), long(kProducers) * kPerProducer);
  EXPECT_EQ(q.size(), 0);
}
