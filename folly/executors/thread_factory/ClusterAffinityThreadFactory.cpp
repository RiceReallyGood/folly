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

#include <pthread.h>
#include <sched.h>

#include <stdexcept>
#include <utility>

#include <glog/logging.h>

#include <folly/executors/ClusterTopology.h>

namespace folly {

ClusterAffinityThreadFactory::ClusterAffinityThreadFactory(
    std::shared_ptr<ThreadFactory> inner, const ClusterTopology& topo)
    : inner_(std::move(inner)), topo_(topo) {
  if (topo_.size() == 0) {
    throw std::invalid_argument("ClusterAffinityThreadFactory: no clusters");
  }
}

ClusterAffinityThreadFactory::ClusterAffinityThreadFactory(
    std::shared_ptr<ThreadFactory> inner,
    const ClusterTopology& topo,
    std::vector<int> cpus)
    : inner_(std::move(inner)), topo_(topo), fixedCpus_(std::move(cpus)) {
  if (fixedCpus_.empty()) {
    throw std::invalid_argument("ClusterAffinityThreadFactory: empty cpuset");
  }
  for (int cpu : fixedCpus_) {
    if (topo_.clusterOfCpu(cpu) < 0) {
      throw std::invalid_argument(
          "ClusterAffinityThreadFactory: CPU " + std::to_string(cpu) +
          " is not in the topology");
    }
  }
  fixedCluster_ = topo_.clusterOfCpu(fixedCpus_.front());
}

std::thread ClusterAffinityThreadFactory::newThread(Func&& func) {
  int cluster = fixedCluster_;
  std::vector<int> cpus = fixedCpus_;
  if (cpus.empty()) {
    cluster = static_cast<int>(next_.fetch_add(1) % topo_.size());
    cpus = topo_.clusters()[cluster].cpus;
  }
  return inner_->newThread(
      [cluster, cpus = std::move(cpus), func = std::move(func)]() mutable {
        cpu_set_t set;
        CPU_ZERO(&set);
        for (int cpu : cpus) {
          CPU_SET(cpu, &set);
        }
        int rc = pthread_setaffinity_np(pthread_self(), sizeof(set), &set);
        if (rc != 0) {
          LOG(FATAL) << "pthread_setaffinity_np(" << formatCpuList(cpus)
                     << ") failed: " << rc;
        }
        setCurrentThreadCluster(cluster);
        func();
      });
}

} // namespace folly
