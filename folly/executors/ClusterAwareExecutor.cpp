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

#include <folly/executors/ClusterAwareExecutor.h>

#include <exception>
#include <utility>

#include <glog/logging.h>

#include <folly/ExceptionString.h>
#include <folly/io/async/Request.h>

namespace folly {

ClusterAwareExecutor::ClusterAwareExecutor(
    size_t numThreads,
    const ClusterTopology& queueTopo,
    std::shared_ptr<ThreadFactory> threadFactory)
    : queue_(queueTopo) {
  threads_.reserve(numThreads);
  for (size_t i = 0; i < numThreads; ++i) {
    threads_.push_back(threadFactory->newThread([this] { run(); }));
  }
}

ClusterAwareExecutor::~ClusterAwareExecutor() {
  // An empty task stops one worker; negative priority puts it behind every
  // queued task.
  for (size_t i = 0; i < threads_.size(); ++i) {
    queue_.addWithPriority(Func(), Executor::LO_PRI);
  }
  for (auto& t : threads_) {
    t.join();
  }
}

void ClusterAwareExecutor::add(Func func) {
  queue_.add([ctx = RequestContext::saveContext(),
              func = std::move(func)]() mutable {
    RequestContextScopeGuard guard(std::move(ctx));
    func();
  });
}

void ClusterAwareExecutor::run() {
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

} // namespace folly
