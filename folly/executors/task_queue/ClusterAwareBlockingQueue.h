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

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <vector>

#include <folly/Optional.h>
#include <folly/concurrency/UnboundedQueue.h>
#include <folly/executors/ClusterTopology.h>
#include <folly/executors/task_queue/BlockingQueue.h>
#include <mutex>

#include <folly/lang/Align.h>
#include <folly/synchronization/Baton.h>
#include <folly/synchronization/MicroSpinLock.h>

namespace folly {

// BlockingQueue with one sub-queue per CPU cluster (see ClusterTopology).
//
// add() enqueues on the caller's cluster and wakes an idle worker of that
// cluster or, if it has none, of the nearest cluster that has one. take()
// runs local tasks first and then steals in the cluster's neighbor order
// (same NUMA node first). Work stays in the producer's cluster when that
// costs no waiting: a task never waits while some worker is idle.
//
// Workers and producers are mapped to clusters with callerCluster(), so pin
// them with ClusterAffinityThreadFactory. Single priority, except that items
// added with a negative priority (CPUThreadPoolExecutor's stop tasks use
// LO_PRI) go to a shared queue taken only after every cluster queue is empty,
// so join() still runs all earlier tasks first.
template <class T>
class ClusterAwareBlockingQueue : public BlockingQueue<T> {
 public:
  struct Stats {
    uint64_t localTakes;
    uint64_t stolenTakes;
  };

  // Keeps a reference to `topo`, which must outlive the queue.
  explicit ClusterAwareBlockingQueue(const ClusterTopology& topo)
      : topo_(topo), subs_(std::max<size_t>(topo.size(), 1)) {}

  BlockingQueueAddResult add(T item) override {
    return addToCluster(callerCluster(topo_), std::move(item));
  }

  BlockingQueueAddResult addWithPriority(T item, int8_t priority) override {
    if (priority >= 0) {
      return add(std::move(item));
    }
    lowPri_.enqueue(std::move(item));
    return wakeIdleWorker(callerCluster(topo_));
  }

  // Returns whether an idle worker was woken for the item.
  BlockingQueueAddResult addToCluster(int cluster, T item) {
    subs_[cluster].queue.enqueue(std::move(item));
    return wakeIdleWorker(cluster);
  }

  T take() override {
    int w = callerCluster(topo_);
    while (true) {
      if (auto item = tryDequeue(w)) {
        return std::move(*item);
      }
      if (auto item = waitIdle(w, [&](Baton<>& baton) {
            baton.wait();
            return true;
          })) {
        return std::move(*item);
      }
    }
  }

  folly::Optional<T> try_take_for(std::chrono::milliseconds time) override {
    auto deadline = std::chrono::steady_clock::now() + time;
    int w = callerCluster(topo_);
    while (true) {
      if (auto item = tryDequeue(w)) {
        return item;
      }
      bool timedOut = false;
      if (auto item = waitIdle(w, [&](Baton<>& baton) {
            timedOut = !baton.try_wait_until(deadline);
            return !timedOut;
          })) {
        return item;
      }
      if (timedOut) {
        return tryDequeue(w);
      }
    }
  }

  size_t size() override {
    size_t n = lowPri_.size();
    for (const auto& sub : subs_) {
      n += sub.queue.size();
    }
    return n;
  }

  size_t sizeOfCluster(int cluster) const { return subs_[cluster].queue.size(); }

  int idleWorkers() const {
    int n = 0;
    for (const auto& sub : subs_) {
      n += sub.idle.load(std::memory_order_relaxed);
    }
    return n;
  }

  Stats stats() const {
    Stats s{0, 0};
    for (const auto& sub : subs_) {
      s.localTakes += sub.localTakes.load(std::memory_order_relaxed);
      s.stolenTakes += sub.stolenTakes.load(std::memory_order_relaxed);
    }
    return s;
  }

 private:
  // An idle worker blocked in take(); lives on that worker's stack.
  struct Waiter {
    Baton<> baton;
    Waiter* prev{nullptr};
    Waiter* next{nullptr};
    bool listed{false};
  };

  struct alignas(hardware_destructive_interference_size) Sub {
    UMPMCQueue<T, false, 6> queue;
    // Idle workers of this cluster, most recent first, guarded by `lock`.
    MicroSpinLock lock{0};
    Waiter* idleHead{nullptr};
    std::atomic<int> idle{0}; // == length of the idle list
    alignas(hardware_destructive_interference_size)
        std::atomic<uint64_t> localTakes{0};
    std::atomic<uint64_t> stolenTakes{0};
  };

  // Wakes an idle worker of `cluster`, else of its nearest neighbor that has
  // one, for an item just enqueued.
  //
  // A producer claims one specific idle worker (unlinks it) and posts that
  // worker's own baton, so back-to-back items wake distinct workers and a
  // claimed wakeup cannot be consumed by another worker.
  BlockingQueueAddResult wakeIdleWorker(int cluster) {
    // Pairs with the fence in waitIdle(): either we see the idle worker or
    // it sees the item when it rechecks.
    std::atomic_thread_fence(std::memory_order_seq_cst);
    if (claimIdle(subs_[cluster])) {
      return BlockingQueueAddResult(true);
    }
    for (int n : neighbors(cluster)) {
      if (claimIdle(subs_[n])) {
        return BlockingQueueAddResult(true);
      }
    }
    return BlockingQueueAddResult(false);
  }

  // Takes the most recently idle worker of `sub` and wakes it.
  static bool claimIdle(Sub& sub) {
    if (sub.idle.load(std::memory_order_relaxed) == 0) {
      return false;
    }
    Waiter* waiter;
    {
      std::lock_guard<MicroSpinLock> g(sub.lock);
      waiter = sub.idleHead;
      if (!waiter) {
        return false;
      }
      unlink(sub, *waiter);
    }
    waiter->baton.post();
    return true;
  }

  // Requires sub.lock.
  static void unlink(Sub& sub, Waiter& w) {
    if (w.prev) {
      w.prev->next = w.next;
    } else {
      sub.idleHead = w.next;
    }
    if (w.next) {
      w.next->prev = w.prev;
    }
    w.listed = false;
    sub.idle.fetch_sub(1, std::memory_order_relaxed);
  }

  const std::vector<int>& neighbors(int cluster) const {
    static const std::vector<int> kNone;
    return static_cast<size_t>(cluster) < topo_.size()
        ? topo_.clusters()[cluster].neighbors
        : kNone;
  }

  folly::Optional<T> tryDequeue(int w) {
    auto& sub = subs_[w];
    if (auto item = sub.queue.try_dequeue()) {
      sub.localTakes.fetch_add(1, std::memory_order_relaxed);
      return item;
    }
    for (int n : neighbors(w)) {
      if (auto item = subs_[n].queue.try_dequeue()) {
        sub.stolenTakes.fetch_add(1, std::memory_order_relaxed);
        return item;
      }
    }
    return lowPri_.try_dequeue();
  }

  // Lists worker `w` as idle, rechecks the queues and, if still empty,
  // blocks on its own baton via `wait` (false = timed out). A worker woken by
  // a producer was already unlinked by it. Otherwise the worker unlinks
  // itself, or, if a producer claimed it meanwhile, waits for that
  // producer's post to its baton. Returns an item found by the recheck,
  // else none (the caller retries).
  template <class Wait>
  folly::Optional<T> waitIdle(int w, Wait&& wait) {
    auto& sub = subs_[w];
    Waiter me;
    {
      std::lock_guard<MicroSpinLock> g(sub.lock);
      me.next = sub.idleHead;
      if (sub.idleHead) {
        sub.idleHead->prev = &me;
      }
      sub.idleHead = &me;
      me.listed = true;
      sub.idle.fetch_add(1, std::memory_order_relaxed);
    }
    std::atomic_thread_fence(std::memory_order_seq_cst);
    auto item = tryDequeue(w);
    if (item || !wait(me.baton)) {
      bool claimed;
      {
        std::lock_guard<MicroSpinLock> g(sub.lock);
        claimed = !me.listed;
        if (!claimed) {
          unlink(sub, me);
        }
      }
      if (claimed) {
        me.baton.wait();
      }
    }
    return item;
  }

  const ClusterTopology& topo_;
  std::vector<Sub> subs_;
  UMPMCQueue<T, false, 6> lowPri_;
};

} // namespace folly
