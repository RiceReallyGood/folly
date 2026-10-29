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

#include <folly/executors/task_queue/ClusterAware2BlockingQueue.h>

#include <atomic>
#include <chrono>
#include <functional>
#include <future>
#include <numeric>
#include <thread>
#include <vector>

#include <folly/executors/ClusterAwareExecutor.h>
#include <folly/executors/ClusterTopology.h>
#include <folly/executors/thread_factory/ClusterAffinityThreadFactory.h>
#include <folly/executors/thread_factory/NamedThreadFactory.h>
#include <gtest/gtest.h>

#include "TestUtil.h"

using namespace folly;
using namespace folly::test;
using namespace std::chrono_literals;

namespace {
// fourClusters(): NUMA node 0 = clusters {0, 1}, node 1 = clusters {2, 3}.
using IntQueue = ClusterAware2BlockingQueue<int>;
using FnQueue = ClusterAware2BlockingQueue<std::function<void()>>;

template <class Buckets>
uint64_t total(const Buckets& b) {
  return std::accumulate(b.begin(), b.end(), uint64_t(0));
}

// Runs function items until it takes an empty one.
void runUntilEmptyItem(FnQueue& q, int home) {
  HomeCluster h(home);
  while (auto f = q.take()) {
    f();
  }
}

// Waits until all `workers` are idle with nothing queued and no handoff token
// left over. A token nobody holds would make every worker probe its cluster
// before each sleep, forever.
template <class Queue>
bool noStaleTokens(Queue& q, int workers) {
  if (q.idleWorkers() != workers || q.size() != 0) {
    return false;
  }
  for (int c = 0; c < 4; ++c) {
    if (q.orphansOf(c) != 0 || q.grabbableOf(c) != 0) {
      return false;
    }
  }
  return true;
}

template <class Queue>
void expectNoStaleTokens(Queue& q, int workers) {
  EXPECT_TRUE(waitUntil([&] { return noStaleTokens(q, workers); }, 5000ms))
      << "queued " << q.size() << " idle " << q.idleWorkers() << " orphans "
      << q.orphansOf(0) << "," << q.orphansOf(1) << "," << q.orphansOf(2)
      << "," << q.orphansOf(3) << " grabbable " << q.grabbableOf(0) << ","
      << q.grabbableOf(1) << "," << q.grabbableOf(2) << "," << q.grabbableOf(3);
}

// Worker A (home aHome) runs a blocker while an item is added to cluster 0,
// which claims worker R (home rHome); A is released right after the claim,
// long before R (asleep) is back on a CPU. Returns in how many of `trials`
// rounds A ran the item.
int runGrabTrials(FnQueue& q, int aHome, int rHome, int trials) {
  std::thread a(runUntilEmptyItem, std::ref(q), aHome);
  std::thread r(runUntilEmptyItem, std::ref(q), rHome);
  const auto aId = a.get_id();
  int onA = 0;
  for (int i = 0; i < trials; ++i) {
    if (!waitUntil([&] { return q.idleWorkers() == 2; }, 1000ms)) {
      ADD_FAILURE() << "workers did not go idle";
      break;
    }
    std::atomic<bool> started{false};
    std::atomic<bool> release{false};
    q.addToCluster(aHome, [&] { // claims A, the only idle worker there
      started = true;
      while (!release.load()) {
      }
    });
    if (!waitUntil([&] { return started.load(); }, 1000ms)) {
      ADD_FAILURE() << "blocker did not start";
      release = true;
      break;
    }
    std::this_thread::sleep_for(10ms); // R is past its spin, asleep
    std::promise<std::thread::id> ran;
    q.addToCluster(0, [&] { ran.set_value(std::this_thread::get_id()); });
    release = true;
    auto f = ran.get_future();
    if (f.wait_for(1s) != std::future_status::ready) {
      ADD_FAILURE() << "item did not run";
      break;
    }
    onA += f.get() == aId;
  }
  q.addToCluster(aHome, {});
  q.addToCluster(rHome, {});
  a.join();
  r.join();
  return onA;
}
} // namespace

TEST(ClusterAware2BlockingQueue, LocalFirst) {
  IntQueue q(fourClusters());
  q.addToCluster(1, 10);
  q.addToCluster(1, 11);
  HomeCluster home(1);
  EXPECT_EQ(q.take(), 10);
  EXPECT_EQ(q.take(), 11);
  auto s = q.stats();
  EXPECT_EQ(s.localTakes, 2);
  EXPECT_EQ(s.orphanAdds, 2);
  EXPECT_EQ(s.orphanTakes, 2); // spent before each take
  EXPECT_EQ(q.orphansOf(1), 0);
}

TEST(ClusterAware2BlockingQueue, OrphanOfAnotherClusterIsTakenBeforeSleeping) {
  IntQueue q(fourClusters());
  q.addToCluster(3, 30);
  HomeCluster home(0);
  EXPECT_EQ(q.take(), 30);
  auto s = q.stats();
  EXPECT_EQ(s.stolenTakes, 1);
  EXPECT_EQ(s.orphanTakes, 1);
  EXPECT_EQ(q.orphansOf(3), 0);
}

// CPUThreadPoolExecutor::join() and the lean executors enqueue their stop
// tasks with LO_PRI and rely on them running after every queued task.
TEST(ClusterAware2BlockingQueue, LowPriorityItemsRunAfterAllClusterQueues) {
  IntQueue q(fourClusters());
  q.addToCluster(3, 1);
  q.addToCluster(1, 2);
  HomeCluster home(0);
  q.addWithPriority(99, Executor::LO_PRI);
  EXPECT_EQ(q.take(), 2);
  EXPECT_EQ(q.take(), 1);
  EXPECT_EQ(q.take(), 99);
  EXPECT_EQ(q.size(), 0);
}

TEST(ClusterAware2BlockingQueue, AddUsesCallerCluster) {
  IntQueue q(fourClusters());
  HomeCluster home(2);
  q.add(5);
  EXPECT_EQ(q.sizeOfCluster(2), 1);
  EXPECT_EQ(q.size(), 1);
}

TEST(ClusterAware2BlockingQueue, TryTakeForTimesOut) {
  IntQueue q(fourClusters());
  HomeCluster home(0);
  auto start = std::chrono::steady_clock::now();
  EXPECT_FALSE(q.try_take_for(20ms).has_value());
  EXPECT_GE(std::chrono::steady_clock::now() - start, 20ms);
  EXPECT_EQ(q.idleWorkers(), 0);
}

TEST(ClusterAware2BlockingQueue, IdleRemoteWorkerIsWokenWhenLocalHasNone) {
  IntQueue q(fourClusters());
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
  auto s = q.stats();
  EXPECT_EQ(s.assignOtherNuma, 1);
  EXPECT_EQ(s.orphanAdds, 0);
  EXPECT_EQ(total(s.wakeRemote), 1);
}

// Back-to-back items must wake distinct idle workers, even when they sit in
// different clusters.
TEST(ClusterAware2BlockingQueue, ConsecutiveItemsWakeDistinctIdleWorkers) {
  IntQueue q(fourClusters());
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
  auto s = q.stats();
  EXPECT_EQ(s.assignSameNuma, 1);
  EXPECT_EQ(s.assignOtherNuma, 1);
}

// A claimed worker must try its target first: the handoff token it holds is
// for the target's item, and nobody else scans that queue.
TEST(ClusterAware2BlockingQueue, ClaimedWorkerTakesItsTargetBeforeItsHomeQueue) {
  IntQueue q(fourClusters());
  std::vector<int> got;
  std::promise<void> finished;
  std::thread w([&] {
    HomeCluster home(2);
    got.push_back(q.take());
    got.push_back(q.take());
    finished.set_value();
  });
  ASSERT_TRUE(waitUntil([&] { return q.idleWorkers() == 1; }, 1000ms));
  std::this_thread::sleep_for(10ms); // past the 2us spin: w is asleep
  q.addToCluster(0, 42); // claims w, target 0
  q.addToCluster(2, 7); // no idle worker left: orphans[2] = 1
  if (finished.get_future().wait_for(1s) != std::future_status::ready) {
    q.addToCluster(2, -1); // w lost the token for 42 and sleeps: wake it
    q.addToCluster(2, -1);
  }
  w.join();
  EXPECT_EQ(got, (std::vector<int>{42, 7}));
  EXPECT_EQ(q.orphansOf(2), 0);
}

TEST(ClusterAware2BlockingQueue, TaskAddedWhileAllWorkersBusyRunsWhenAnyWorkerFrees) {
  FnQueue q(fourClusters());
  std::thread w(runUntilEmptyItem, std::ref(q), 2);
  ASSERT_TRUE(waitUntil([&] { return q.idleWorkers() == 1; }, 1000ms));
  std::promise<void> unblock;
  auto unblocked = unblock.get_future().share();
  std::atomic<bool> busy{false};
  q.addToCluster(2, [&] {
    busy = true;
    unblocked.wait();
  });
  ASSERT_TRUE(waitUntil([&] { return busy.load(); }, 1000ms));
  std::promise<void> ran;
  q.addToCluster(0, [&] { ran.set_value(); }); // no idle worker anywhere
  EXPECT_EQ(q.stats().orphanAdds, 1);
  unblock.set_value();
  EXPECT_EQ(ran.get_future().wait_for(1s), std::future_status::ready);
  EXPECT_EQ(q.stats().orphanTakes, 1);
  q.addToCluster(2, {});
  w.join();
}

// CA2 never scans other clusters' queues, so an awake worker leaves an item
// that was assigned to a remote worker to that worker.
TEST(ClusterAware2BlockingQueue, AwakeWorkerNeverTakesRemotelyAssignedItem) {
  FnQueue q(fourClusters());
  int onA = 0;
  runWithTimeout([&] { onA = runGrabTrials(q, 1, 2, 20); }, 60s);
  EXPECT_EQ(onA, 0);
  EXPECT_EQ(q.stats().grabTakes, 0);
}

// CA2-G: an awake worker of the same NUMA node takes the item instead of
// waiting for the claimed remote worker to come back on a CPU, and the
// claimed worker, finding the token spent, skips the useless probe.
TEST(ClusterAware2GrabBlockingQueue, AwakeSameNumaWorkerGrabsRemotelyAssignedItem) {
  FnQueue q(fourClusters(), ClusterAware2Options{true});
  int onA = 0;
  runWithTimeout([&] { onA = runGrabTrials(q, 1, 2, 20); }, 60s);
  EXPECT_GE(onA, 1);
  auto s = q.stats();
  EXPECT_GE(s.grabTakes, 1);
  EXPECT_GE(s.grabSkips, 1);
  EXPECT_EQ(q.grabbableOf(0), 0);
}

TEST(ClusterAware2GrabBlockingQueue, GrabIsLimitedToSameNuma) {
  FnQueue q(fourClusters(), ClusterAware2Options{true});
  int onA = 0;
  runWithTimeout([&] { onA = runGrabTrials(q, 2, 1, 20); }, 60s);
  EXPECT_EQ(onA, 0);
  EXPECT_EQ(q.stats().grabTakes, 0);
}

// Hooks that pause a worker at one interleaving point so that a test can
// slip producer and worker actions in between. Set before the threads start
// and cleared (HookReset) after they are joined.
struct TestHooks {
  static inline std::function<void(int)> ownQueueTake;
  static inline std::function<void(int)> lowPri;
  static inline std::function<void(int)> claimUnlink;
  static void afterOwnQueueTake(int c) {
    if (ownQueueTake) {
      ownQueueTake(c);
    }
  }
  static void beforeLowPri(int c) {
    if (lowPri) {
      lowPri(c);
    }
  }
  static void afterClaimUnlink(int c) {
    if (claimUnlink) {
      claimUnlink(c);
    }
  }
};

struct HookReset {
  ~HookReset() {
    TestHooks::ownQueueTake = nullptr;
    TestHooks::lowPri = nullptr;
    TestHooks::claimUnlink = nullptr;
  }
};

using HookedQueue = ClusterAware2BlockingQueue<int, TestHooks>;

// Pauses the `nth` call of a hook until resume() is called.
class PauseAt {
 public:
  explicit PauseAt(int nth) : nth_(nth) {}
  std::function<void(int)> hook() {
    return [this](int) {
      if (++calls_ == nth_) {
        paused_.set_value();
        resume_.wait();
      }
    };
  }
  void waitPaused() { paused_.get_future().wait(); }
  void resume() { resumePromise_.set_value(); }

 private:
  const int nth_;
  std::atomic<int> calls_{0};
  std::promise<void> paused_;
  std::promise<void> resumePromise_;
  std::shared_future<void> resume_{resumePromise_.get_future().share()};
};

// Takes items until a negative one (a stop item), which it records too.
std::vector<int> takeUntilStop(HookedQueue& q, int home) {
  HomeCluster h(home);
  std::vector<int> got;
  int v;
  do {
    v = q.take();
    got.push_back(v);
  } while (v >= 0);
  return got;
}

// A worker that took an item from its own queue must not then spend a token
// that belongs to an item enqueued meanwhile: that item would stay queued
// with nobody to take it while workers sleep.
TEST(ClusterAware2Interleaving, LocalTakeNeverSpendsALaterItemsToken) {
  HookReset reset;
  HookedQueue q(fourClusters());
  q.addToCluster(1, 1); // no worker anywhere: orphans[1] = 1
  PauseAt pause(1);
  TestHooks::ownQueueTake = pause.hook();
  std::promise<int> got;
  std::thread l([&] {
    HomeCluster home(1);
    got.set_value(q.take());
  });
  pause.waitPaused(); // l has taken item 1 from its own queue
  {
    HomeCluster home(0); // a worker elsewhere about to sleep, finding nothing
    EXPECT_FALSE(q.try_take_for(0ms).has_value());
  }
  q.addToCluster(1, 2); // still no idle worker: item 2 gets orphans[1]
  pause.resume();
  EXPECT_EQ(got.get_future().get(), 1);
  l.join();
  EXPECT_EQ(q.orphansOf(1), 1) << "item 2 lost its token";
  HomeCluster home(0);
  auto item = q.try_take_for(0ms);
  EXPECT_TRUE(item.has_value() && *item == 2);
}

// try_take_for times out while a producer is between claiming the worker
// and waking it: the worker must still take the item it was claimed for.
TEST(ClusterAware2Interleaving, TimedOutWorkerThatWasClaimedTakesItsItem) {
  HookReset reset;
  HookedQueue q(fourClusters());
  std::atomic<bool> delay{true};
  TestHooks::claimUnlink = [&](int) {
    if (delay.exchange(false)) {
      std::this_thread::sleep_for(100ms); // the worker's 20ms run out
    }
  };
  std::promise<folly::Optional<int>> got;
  std::promise<void> mayExit; // keeps w's stack alive if the item is lost
  std::thread w([&] {
    HomeCluster home(2);
    got.set_value(q.try_take_for(20ms));
    mayExit.get_future().wait();
  });
  ASSERT_TRUE(waitUntil([&] { return q.idleWorkers() == 1; }, 1000ms));
  q.addToCluster(0, 42); // claims w, wakes it 100ms later
  auto f = got.get_future();
  bool ready = f.wait_for(1s) == std::future_status::ready;
  EXPECT_TRUE(ready);
  if (ready) {
    auto item = f.get();
    EXPECT_TRUE(item.has_value() && *item == 42);
  }
  mayExit.set_value();
  w.join();
}

// The only worker, claimed for an item while it rechecks, finds a stop item
// (LO_PRI) instead: it must run the item it was claimed for before stopping,
// or the item is never run.
TEST(ClusterAware2Interleaving, ClaimedWorkerRunsItsItemBeforeStopping) {
  HookReset reset;
  HookedQueue q(fourClusters());
  PauseAt pause(2); // 1: first pass, 2: recheck after listing as idle
  TestHooks::lowPri = pause.hook();
  std::vector<int> got;
  std::thread w([&] { got = takeUntilStop(q, 2); });
  pause.waitPaused();
  q.addToCluster(0, 42); // claims w, which is listed
  q.addWithPriority(-1, Executor::LO_PRI); // nobody idle to claim
  pause.resume();
  w.join();
  EXPECT_EQ(got, (std::vector<int>{42, -1}));
}

// The only worker found no item and no token, and is about to read the
// LO_PRI queue, when an item (an orphan: nobody idle) and then a stop item
// are added: it must run the item before stopping.
TEST(ClusterAware2Interleaving, StopItemNeverOvertakesAnEarlierItem) {
  HookReset reset;
  HookedQueue q(fourClusters());
  PauseAt pause(1);
  TestHooks::lowPri = pause.hook();
  std::vector<int> got;
  std::thread w([&] { got = takeUntilStop(q, 2); });
  pause.waitPaused();
  q.addToCluster(0, 42); // w is not listed: orphans[0] = 1
  q.addWithPriority(-1, Executor::LO_PRI);
  pause.resume();
  w.join();
  EXPECT_EQ(got, (std::vector<int>{42, -1}));
}

class ClusterAware2Stress : public ::testing::TestWithParam<bool> {
 protected:
  ClusterAware2Options opts() const { return ClusterAware2Options{GetParam()}; }
};

// Work conservation means at once, not eventually: a burst from one cluster
// must occupy every idle worker, wherever it lives.
TEST_P(ClusterAware2Stress, BurstFromOneClusterOccupiesEveryIdleWorker) {
  constexpr int kWorkers = 8;
  FnQueue q(fourClusters(), opts());
  std::vector<std::thread> workers;
  for (int i = 0; i < kWorkers; ++i) {
    workers.emplace_back(runUntilEmptyItem, std::ref(q), i / 2);
  }
  ASSERT_TRUE(waitUntil([&] { return q.idleWorkers() == kWorkers; }, 1000ms));
  std::atomic<int> arrived{0};
  std::atomic<int> together{0};
  std::atomic<int> done{0};
  for (int i = 0; i < kWorkers; ++i) {
    q.addToCluster(0, [&] {
      ++arrived;
      auto deadline = std::chrono::steady_clock::now() + 2s;
      while (arrived.load() < kWorkers &&
             std::chrono::steady_clock::now() < deadline) {
      }
      together += arrived.load() == kWorkers;
      ++done;
    });
  }
  EXPECT_TRUE(waitUntil([&] { return done.load() == kWorkers; }, 5000ms));
  EXPECT_EQ(together.load(), kWorkers);
  for (int i = 0; i < kWorkers; ++i) {
    q.addToCluster(0, {});
  }
  for (auto& t : workers) {
    t.join();
  }
}

TEST_P(ClusterAware2Stress, DestroyingExecutorRunsEveryQueuedTask) {
  constexpr int kProducers = 8;
  constexpr int kPerProducer = 100000;
  std::atomic<long> done{0};
  runWithTimeout(
      [&] {
        ClusterAware2Executor ex(
            16,
            fourClusters(),
            std::make_shared<ClusterAffinityThreadFactory>(
                std::make_shared<NamedThreadFactory>("ca2"), fourClusters()),
            opts());
        std::vector<std::thread> producers;
        for (int p = 0; p < kProducers; ++p) {
          producers.emplace_back([&, p] {
            HomeCluster home(p % 4);
            for (int n = 0; n < kPerProducer; ++n) {
              ex.add([&] { ++done; });
            }
          });
        }
        for (auto& t : producers) {
          t.join();
        }
      }, // ~ClusterAware2Executor runs everything still queued
      60s);
  EXPECT_EQ(done.load(), long(kProducers) * kPerProducer);
}

TEST_P(ClusterAware2Stress, StressNoLostTasks) {
  constexpr int kProducers = 8;
  constexpr int kWorkers = 16;
  constexpr int kPerProducer = 200000;
  IntQueue q(fourClusters(), opts());
  std::atomic<long> sum{0};
  runWithTimeout(
      [&] {
        std::vector<std::thread> workers;
        for (int i = 0; i < kWorkers; ++i) {
          workers.emplace_back([&, i] {
            HomeCluster home(i % 4);
            long mine = 0;
            while (true) {
              int v;
              if (i % 2 == 0) {
                v = q.take();
              } else {
                // Times out at once: races every claim against the timeout.
                auto item = q.try_take_for(0ms);
                if (!item) {
                  continue;
                }
                v = *item;
              }
              if (v < 0) {
                break;
              }
              mine += v;
            }
            sum += mine;
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
  EXPECT_EQ(sum.load(), long(kProducers) * kPerProducer);
  EXPECT_EQ(q.size(), 0);
}

// A worker whose recheck found an item after a producer claimed it must hand
// its token on. Cluster 3 has producers but no worker, so an item there whose
// token was dropped would stay queued for good. Runs until the handoff path
// was taken at least once.
TEST_P(ClusterAware2Stress, ClaimRaceExercisesHandoff) {
  constexpr int kProducers = 8;
  constexpr int kWorkers = 16;
  constexpr int kPerProducer = 200000;
  uint64_t handoffs = 0;
  for (int round = 0; round < 5 && handoffs == 0; ++round) {
    IntQueue q(fourClusters(), opts());
    std::atomic<long> sum{0};
    runWithTimeout(
        [&] {
          std::vector<std::thread> workers;
          for (int i = 0; i < kWorkers; ++i) {
            workers.emplace_back([&, i] {
              HomeCluster home(i % 3);
              long mine = 0;
              for (int v; (v = q.take()) >= 0;) {
                mine += v;
              }
              sum += mine;
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
          expectNoStaleTokens(q, kWorkers);
          for (int i = 0; i < kWorkers; ++i) {
            q.addToCluster(i % 3, -1);
          }
          for (auto& t : workers) {
            t.join();
          }
        },
        60s);
    EXPECT_EQ(sum.load(), long(kProducers) * kPerProducer);
    handoffs += q.stats().handoffs;
  }
  EXPECT_GT(handoffs, 0u);
}

INSTANTIATE_TEST_SUITE_P(
    Policies,
    ClusterAware2Stress,
    ::testing::Values(false, true),
    [](const ::testing::TestParamInfo<bool>& info) {
      return info.param ? "Grab" : "NoGrab";
    });
