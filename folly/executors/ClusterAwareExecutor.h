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

#include <exception>
#include <memory>
#include <thread>
#include <utility>
#include <vector>

#include <glog/logging.h>

#include <folly/ExceptionString.h>
#include <folly/Executor.h>
#include <folly/executors/task_queue/ClusterAwareBlockingQueue.h>
#include <folly/executors/thread_factory/ThreadFactory.h>
#include <folly/io/async/Request.h>

namespace folly {

// A lean fixed-size pool whose workers run tasks from a cluster-aware
// `Queue` (e.g. ClusterAwareBlockingQueue<Func>) keyed by `queueTopo`. Unlike
// CPUThreadPoolExecutor it keeps no per-task bookkeeping and does not count
// keep-alives, so the only state shared between producers and workers is the
// queue. With a single-cluster `queueTopo` it is a plain global-queue pool.
// The RequestContext of add() is restored while the task runs.
template <class Queue>
class BasicClusterAwareExecutor : public Executor {
 public:
  using Stats = typename Queue::Stats;

  // `queueTopo` must outlive the executor; `threadFactory` decides placement
  // (e.g. ClusterAffinityThreadFactory). `queueArgs` follow the topology in
  // the queue's constructor.
  template <class... QueueArgs>
  BasicClusterAwareExecutor(
      size_t numThreads,
      const ClusterTopology& queueTopo,
      std::shared_ptr<ThreadFactory> threadFactory,
      QueueArgs&&... queueArgs)
      : queue_(queueTopo, std::forward<QueueArgs>(queueArgs)...) {
    threads_.reserve(numThreads);
    for (size_t i = 0; i < numThreads; ++i) {
      threads_.push_back(threadFactory->newThread([this] { run(); }));
    }
  }

  // Runs every task still queued, then joins the workers.
  ~BasicClusterAwareExecutor() override {
    // An empty task stops one worker; negative priority puts it behind every
    // queued task.
    for (size_t i = 0; i < threads_.size(); ++i) {
      queue_.addWithPriority(Func(), Executor::LO_PRI);
    }
    for (auto& t : threads_) {
      t.join();
    }
  }

  void add(Func func) override {
    queue_.add([ctx = RequestContext::saveContext(),
                func = std::move(func)]() mutable {
      RequestContextScopeGuard guard(std::move(ctx));
      func();
    });
  }

  Stats stats() const { return queue_.stats(); }

 private:
  void run() {
    while (true) {
      auto task = queue_.take();
      if (!task) {
        return;
      }
      try {
        task();
      } catch (const std::exception& e) {
        LOG(ERROR) << "ClusterAwareExecutor: task threw: " << exceptionStr(e);
      } catch (...) {
        LOG(ERROR) << "ClusterAwareExecutor: task threw a non-exception";
      }
    }
  }

  Queue queue_;
  std::vector<std::thread> threads_;
};

using ClusterAwareExecutor =
    BasicClusterAwareExecutor<ClusterAwareBlockingQueue<Func>>;

} // namespace folly
