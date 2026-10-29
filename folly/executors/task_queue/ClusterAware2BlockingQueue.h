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
#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <mutex>
#include <vector>

#include <folly/Optional.h>
#include <folly/chrono/Hardware.h>
#include <folly/concurrency/UnboundedQueue.h>
#include <folly/executors/ClusterTopology.h>
#include <folly/executors/task_queue/BlockingQueue.h>
#include <folly/lang/Align.h>
#include <folly/synchronization/Baton.h>
#include <folly/synchronization/MicroSpinLock.h>

namespace folly {

struct ClusterAware2Options {
  // An awake idle worker may take an item of another cluster of its NUMA
  // node that was assigned to a remote worker which is not running yet.
  bool grab = false;
};

// BlockingQueue with one sub-queue per CPU cluster (see ClusterTopology).
// Same scheduling as ClusterAwareBlockingQueue: add() enqueues on the
// caller's cluster and claims an idle worker of that cluster or else of the
// nearest cluster that has one; a task never waits while some worker is
// idle. Unlike it, a worker never probes other clusters' queues to find
// work; it only reads per-cluster counters that change rarely.
//
// Every item gets exactly one handoff token when it is enqueued on cluster
// c: either a claimed idle worker told to take from c ("target"), or, when
// no worker is idle, orphans[c] += 1. A token is spent by one attempt to
// take from c's queue, so at all times
//
//   items in c's queue <= unspent claims targeting c + orphans[c].
//
// A claimed worker first tries its target; every worker reads all orphans[]
// before it sleeps. Two seq_cst fence pairings close the races:
//   A: producer "enqueue, fence, look for idle workers" against worker
//      "list as idle, fence, recheck the queues";
//   B: producer "orphans[c] += 1, fence, look again" against worker
//      "list as idle, fence, read orphans[]".
// A claimed worker that already found another item in its recheck hands its
// token on (claims another worker for the target, or counts an orphan).
//
// Workers and producers are mapped to clusters with callerCluster(), so pin
// them with ClusterAffinityThreadFactory. Items added with a negative
// priority (stop tasks) go to a shared queue taken only after every cluster
// queue and orphan count is empty.
template <class T>
class ClusterAware2BlockingQueue : public BlockingQueue<T> {
 public:
  static constexpr size_t kWakeBuckets = 11;
  // Upper bounds of the first 10 buckets; the last bucket is open-ended.
  static constexpr std::array<uint64_t, kWakeBuckets - 1> kWakeBucketUpperNs{
      {500, 1000, 2000, 4000, 8000, 16000, 32000, 64000, 128000, 256000}};

  struct Stats {
    uint64_t localTakes; // from the worker's own cluster queue
    uint64_t stolenTakes; // from another cluster's queue
    // Claims with a target, by where the claimed worker lives relative to it.
    uint64_t assignLocal;
    uint64_t assignSameNuma;
    uint64_t assignOtherNuma;
    uint64_t orphanAdds;
    uint64_t orphanLocalDecs; // spent by a local take that found an item
    uint64_t orphanTakes; // spent on another attempt that found an item
    uint64_t orphanMisses; // spent on an attempt that found the queue empty
    uint64_t emptyTargetWakes; // claimed worker found its target empty
    uint64_t handoffs;
    uint64_t grabTakes;
    uint64_t grabMisses;
    uint64_t grabSkips; // claimed worker whose token another worker spent
    // Claim to the claimed worker running again, by kWakeBucketUpperNs.
    std::array<uint64_t, kWakeBuckets> wakeLocal;
    std::array<uint64_t, kWakeBuckets> wakeRemote;
  };

  // Keeps a reference to `topo`, which must outlive the queue.
  explicit ClusterAware2BlockingQueue(
      const ClusterTopology& topo, ClusterAware2Options opts = {})
      : topo_(topo),
        opts_(opts),
        subs_(std::max<size_t>(topo.size(), 1)),
        numa_(subs_.size(), 0) {
    for (size_t c = 0; c < topo.size(); ++c) {
      numa_[c] = topo.clusters()[c].numaNode;
    }
  }

  BlockingQueueAddResult add(T item) override {
    return addToCluster(callerCluster(topo_), std::move(item));
  }

  BlockingQueueAddResult addWithPriority(T item, int8_t priority) override {
    if (priority >= 0) {
      return add(std::move(item));
    }
    lowPri_.enqueue(std::move(item));
    // Pairing A on the shared queue: every worker reads it before sleeping.
    std::atomic_thread_fence(std::memory_order_seq_cst);
    return BlockingQueueAddResult(claimFor(callerCluster(topo_), kNoTarget));
  }

  // Returns whether an idle worker was claimed for the item.
  BlockingQueueAddResult addToCluster(int cluster, T item) {
    subs_[cluster].queue.enqueue(std::move(item));
    return BlockingQueueAddResult(assignToken(cluster));
  }

  T take() override {
    return std::move(*takeOrWait([](Baton<>& baton) {
      baton.wait();
      return true;
    }));
  }

  folly::Optional<T> try_take_for(std::chrono::milliseconds time) override {
    auto deadline = std::chrono::steady_clock::now() + time;
    return takeOrWait(
        [&](Baton<>& baton) { return baton.try_wait_until(deadline); });
  }

  size_t size() override {
    size_t n = lowPri_.size();
    for (const auto& sub : subs_) {
      n += sub.queue.size();
    }
    return n;
  }

  size_t sizeOfCluster(int cluster) const { return subs_[cluster].queue.size(); }

  // Walks every idle list under its lock; for tests.
  int idleWorkers() const {
    int n = 0;
    for (auto& sub : subs_) {
      std::lock_guard<MicroSpinLock> g(sub.lock);
      for (Waiter* w = sub.idleHead; w; w = w->next) {
        ++n;
      }
    }
    return n;
  }

  int64_t orphansOf(int cluster) const {
    return subs_[cluster].orphans.load(std::memory_order_relaxed);
  }

  Stats stats() const {
    Stats s{};
    auto sum = [](uint64_t& to, const std::atomic<uint64_t>& from) {
      to += from.load(std::memory_order_relaxed);
    };
    for (const auto& sub : subs_) {
      const Counters& c = sub.counters;
      sum(s.localTakes, c.localTakes);
      sum(s.stolenTakes, c.stolenTakes);
      sum(s.assignLocal, c.assignLocal);
      sum(s.assignSameNuma, c.assignSameNuma);
      sum(s.assignOtherNuma, c.assignOtherNuma);
      sum(s.orphanAdds, c.orphanAdds);
      sum(s.orphanLocalDecs, c.orphanLocalDecs);
      sum(s.orphanTakes, c.orphanTakes);
      sum(s.orphanMisses, c.orphanMisses);
      sum(s.emptyTargetWakes, c.emptyTargetWakes);
      sum(s.handoffs, c.handoffs);
      sum(s.grabTakes, c.grabTakes);
      sum(s.grabMisses, c.grabMisses);
      sum(s.grabSkips, c.grabSkips);
      for (size_t i = 0; i < kWakeBuckets; ++i) {
        sum(s.wakeLocal[i], c.wakeLocal[i]);
        sum(s.wakeRemote[i], c.wakeRemote[i]);
      }
    }
    return s;
  }

 private:
  static constexpr int kNoTarget = -1; // claimed for a LO_PRI item

  // An idle worker blocked in take(); lives on that worker's stack. A
  // producer that claims it writes `target` and `claimedAt` before the post.
  struct Waiter {
    Baton<> baton;
    Waiter* prev{nullptr};
    Waiter* next{nullptr};
    bool listed{false};
    int target{kNoTarget};
    uint64_t claimedAt{0};
  };

  // Counted where the acting thread works (producer: the item's cluster;
  // worker: its home cluster).
  struct Counters {
    std::atomic<uint64_t> localTakes{0};
    std::atomic<uint64_t> stolenTakes{0};
    std::atomic<uint64_t> assignLocal{0};
    std::atomic<uint64_t> assignSameNuma{0};
    std::atomic<uint64_t> assignOtherNuma{0};
    std::atomic<uint64_t> orphanAdds{0};
    std::atomic<uint64_t> orphanLocalDecs{0};
    std::atomic<uint64_t> orphanTakes{0};
    std::atomic<uint64_t> orphanMisses{0};
    std::atomic<uint64_t> emptyTargetWakes{0};
    std::atomic<uint64_t> handoffs{0};
    std::atomic<uint64_t> grabTakes{0};
    std::atomic<uint64_t> grabMisses{0};
    std::atomic<uint64_t> grabSkips{0};
    std::array<std::atomic<uint64_t>, kWakeBuckets> wakeLocal{};
    std::array<std::atomic<uint64_t>, kWakeBuckets> wakeRemote{};
  };

  // Each part on its own cache line: other clusters read hasIdle and orphans
  // often and must not be invalidated by this cluster's queue traffic.
  struct alignas(hardware_destructive_interference_size) Sub {
    UMPMCQueue<T, false, 6> queue;
    // Idle workers of this cluster, most recent first, guarded by `lock`.
    alignas(hardware_destructive_interference_size) mutable MicroSpinLock
        lock{0};
    Waiter* idleHead{nullptr};
    // Whether the idle list is non-empty. Written under `lock`, and only when
    // the list turns empty or non-empty. A worker that lists itself behind
    // others does not write it; pairing A still holds because the store of
    // `true` happens before that worker's fence through the lock (C++20
    // [atomics.order] p4.4).
    alignas(hardware_destructive_interference_size) std::atomic<bool> hasIdle{
        false};
    alignas(hardware_destructive_interference_size) std::atomic<int64_t>
        orphans{0};
    alignas(hardware_destructive_interference_size) Counters counters;
  };

  static void bump(std::atomic<uint64_t>& counter) {
    counter.fetch_add(1, std::memory_order_relaxed);
  }

  // Decrements a positive token count; false if it was zero.
  static bool tryDec(std::atomic<int64_t>& count) {
    int64_t v = count.load(std::memory_order_relaxed);
    while (v > 0) {
      if (count.compare_exchange_weak(
              v, v - 1, std::memory_order_acquire, std::memory_order_relaxed)) {
        return true;
      }
    }
    return false;
  }

  const std::vector<int>& neighbors(int cluster) const {
    static const std::vector<int> kNone;
    return static_cast<size_t>(cluster) < topo_.size()
        ? topo_.clusters()[cluster].neighbors
        : kNone;
  }

  // Gives an item on cluster `c` its token: a claimed idle worker, or else
  // orphans[c] += 1 and one more look for an idle worker (a second claim only
  // adds a token). Returns whether a worker was claimed.
  bool assignToken(int c) {
    // Pairing A: we see the idle worker or its recheck sees the item.
    std::atomic_thread_fence(std::memory_order_seq_cst);
    if (claimFor(c, c)) {
      return true;
    }
    subs_[c].orphans.fetch_add(1, std::memory_order_seq_cst);
    bump(subs_[c].counters.orphanAdds);
    // Pairing B: we see a worker that listed itself meanwhile, or its
    // recheck sees orphans[c].
    std::atomic_thread_fence(std::memory_order_seq_cst);
    return claimFor(c, c);
  }

  // Claims an idle worker of `from`, else of its nearest neighbor that has
  // one, and sends it to `target`.
  bool claimFor(int from, int target) {
    if (claimIdle(from, target)) {
      return true;
    }
    for (int n : neighbors(from)) {
      if (claimIdle(n, target)) {
        return true;
      }
    }
    return false;
  }

  // Takes the most recently idle worker of cluster `d` and wakes it.
  bool claimIdle(int d, int target) {
    Sub& sub = subs_[d];
    if (!sub.hasIdle.load(std::memory_order_relaxed)) {
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
    if (target != kNoTarget) {
      Counters& c = subs_[target].counters;
      bump(
          d == target                   ? c.assignLocal
              : numa_[d] == numa_[target] ? c.assignSameNuma
                                          : c.assignOtherNuma);
      waiter->claimedAt = hardware_timestamp();
    }
    waiter->target = target;
    waiter->baton.post();
    return true;
  }

  // Requires sub.lock.
  static void link(Sub& sub, Waiter& w) {
    w.next = sub.idleHead;
    if (sub.idleHead) {
      sub.idleHead->prev = &w;
    } else {
      sub.hasIdle.store(true, std::memory_order_relaxed);
    }
    sub.idleHead = &w;
    w.listed = true;
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
    if (!sub.idleHead) {
      sub.hasIdle.store(false, std::memory_order_relaxed);
    }
  }

  // Unlinks `me` unless a producer claimed it; returns whether it did.
  bool unlistSelf(int h, Waiter& me) {
    Sub& sub = subs_[h];
    std::lock_guard<MicroSpinLock> g(sub.lock);
    if (!me.listed) {
      return false;
    }
    unlink(sub, me);
    return true;
  }

  // Worker `h` was claimed and is running again: spends its token on the
  // target (step 1 of the design).
  folly::Optional<T> takeTarget(int h, const Waiter& me) {
    const int t = me.target;
    if (t == kNoTarget) {
      return folly::none;
    }
    Counters& c = subs_[h].counters;
    recordWake(c, t == h, me.claimedAt);
    if (auto item = subs_[t].queue.try_dequeue()) {
      bump(t == h ? c.localTakes : c.stolenTakes);
      return item;
    }
    bump(c.emptyTargetWakes);
    return folly::none;
  }

  // Steps 2-4: own queue, then orphaned items (own cluster, then by
  // distance), then LO_PRI items.
  folly::Optional<T> tryOthers(int h) {
    Sub& sub = subs_[h];
    if (auto item = sub.queue.try_dequeue()) {
      bump(sub.counters.localTakes);
      // The item may have been an orphan; spending its token saves a remote
      // worker a useless probe. Safe: both sides of the invariant drop by 1.
      if (tryDec(sub.orphans)) {
        bump(sub.counters.orphanLocalDecs);
      }
      return item;
    }
    if (auto item = takeOrphan(h, h)) {
      return item;
    }
    for (int d : neighbors(h)) {
      if (auto item = takeOrphan(h, d)) {
        return item;
      }
    }
    return lowPri_.try_dequeue();
  }

  folly::Optional<T> takeOrphan(int h, int d) {
    if (!tryDec(subs_[d].orphans)) {
      return folly::none;
    }
    Counters& c = subs_[h].counters;
    if (auto item = subs_[d].queue.try_dequeue()) {
      bump(c.orphanTakes);
      bump(d == h ? c.localTakes : c.stolenTakes);
      return item;
    }
    bump(c.orphanMisses);
    return folly::none;
  }

  // A claimed worker that will not try its target passes the token on.
  void handOff(int h, const Waiter& me) {
    if (me.target == kNoTarget) {
      return; // LO_PRI: the worker reads the shared queue before sleeping
    }
    bump(subs_[h].counters.handoffs);
    assignToken(me.target);
  }

  static void recordWake(Counters& c, bool local, uint64_t claimedAt) {
    uint64_t ns = ticksToNs(hardware_timestamp() - claimedAt);
    size_t i = 0;
    while (i < kWakeBucketUpperNs.size() && ns > kWakeBucketUpperNs[i]) {
      ++i;
    }
    bump(local ? c.wakeLocal[i] : c.wakeRemote[i]);
  }

  static uint64_t ticksToNs(uint64_t ticks) {
#if defined(__aarch64__)
    static const double nsPerTick = [] {
      uint64_t freq;
      asm volatile("mrs %0, cntfrq_el0" : "=r"(freq));
      return 1e9 / double(freq);
    }();
    return uint64_t(double(ticks) * nsPerTick);
#else
    return ticks; // only the aarch64 counter has a known frequency
#endif
  }

  // Runs the worker steps until an item turns up. `wait(baton)` blocks until
  // a producer claims this worker, or returns false on timeout; then one
  // more attempt is made and its result returned.
  template <class Wait>
  folly::Optional<T> takeOrWait(Wait&& wait) {
    const int h = callerCluster(topo_);
    if (auto item = tryOthers(h)) {
      return item;
    }
    while (true) {
      Waiter me;
      {
        std::lock_guard<MicroSpinLock> g(subs_[h].lock);
        link(subs_[h], me);
      }
      // Pairings A and B, worker side.
      std::atomic_thread_fence(std::memory_order_seq_cst);
      if (auto item = tryOthers(h)) {
        if (!unlistSelf(h, me)) {
          // Claimed meanwhile: wait until the producer is done with `me`,
          // then pass on the token, which this worker will not spend.
          me.baton.wait();
          handOff(h, me);
        }
        return item;
      }
      const bool posted = wait(me.baton);
      if (!posted) {
        if (unlistSelf(h, me)) {
          return tryOthers(h);
        }
        me.baton.wait(); // claimed while timing out: spend the token below
      }
      if (auto item = takeTarget(h, me)) {
        return item;
      }
      if (auto item = tryOthers(h)) {
        return item;
      }
      if (!posted) {
        return folly::none;
      }
    }
  }

  const ClusterTopology& topo_;
  const ClusterAware2Options opts_;
  std::vector<Sub> subs_;
  std::vector<int> numa_; // NUMA node of each cluster
  UMPMCQueue<T, false, 6> lowPri_;
};

} // namespace folly
