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

#include <memory>
#include <thread>
#include <vector>

#include <folly/Executor.h>
#include <folly/executors/task_queue/ClusterAwareBlockingQueue.h>
#include <folly/executors/thread_factory/ThreadFactory.h>

namespace folly {

// A lean fixed-size pool whose workers run tasks from a
// ClusterAwareBlockingQueue keyed by `queueTopo`. Unlike
// CPUThreadPoolExecutor it keeps no per-task bookkeeping and does not count
// keep-alives, so the only state shared between producers and workers is the
// queue. With a single-cluster `queueTopo` it is a plain global-queue pool.
// The RequestContext of add() is restored while the task runs.
class ClusterAwareExecutor : public Executor {
 public:
  using Stats = typename ClusterAwareBlockingQueue<Func>::Stats;

  // `queueTopo` must outlive the executor; `threadFactory` decides placement
  // (e.g. ClusterAffinityThreadFactory).
  ClusterAwareExecutor(
      size_t numThreads,
      const ClusterTopology& queueTopo,
      std::shared_ptr<ThreadFactory> threadFactory);

  // Runs every task still queued, then joins the workers.
  ~ClusterAwareExecutor() override;

  void add(Func func) override;

  Stats stats() const { return queue_.stats(); }

 private:
  void run();

  ClusterAwareBlockingQueue<Func> queue_;
  std::vector<std::thread> threads_;
};

} // namespace folly
