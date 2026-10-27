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

#include <cstddef>
#include <string>
#include <vector>

#include <folly/Range.h>

namespace folly {

// CPUs that share a cluster (e.g. on Kunpeng 950: 8 cores / 16 threads that
// share one L3 slice), restricted to the CPUs this process may run on.
struct CpuCluster {
  int id; // dense index, clusters ordered by their lowest CPU
  int sysClusterId; // kernel cluster_id, or the L3 grouping key
  int numaNode; // NUMA node of the cluster's first CPU
  std::vector<int> cpus; // allowed CPUs, ascending
  // Other clusters in steal order: same NUMA node first, then by NUMA
  // distance; ties by id.
  std::vector<int> neighbors;
};

// CPU cluster / NUMA topology read from sysfs.
class ClusterTopology {
 public:
  // Groups `allowedCpus` (the calling thread's affinity when empty) by
  // <sysRoot>/cpu/cpuN/topology/cluster_id, falling back to the L3 sharing in
  // cpu/cpuN/cache/index3/shared_cpu_list and then to a single group. NUMA
  // nodes and distances come from <sysRoot>/node/nodeK/{cpulist,distance}.
  static ClusterTopology detect(
      const std::string& sysRoot, std::vector<int> allowedCpus);

  // Detected once from /sys/devices/system and the process affinity; never
  // destroyed.
  static const ClusterTopology& instance();

  const std::vector<CpuCluster>& clusters() const { return clusters_; }
  size_t size() const { return clusters_.size(); }

  // Dense cluster id of `cpu`, or -1 if `cpu` is not in the topology.
  int clusterOfCpu(int cpu) const;

  // One line per cluster:
  // "cluster <id> sys=<sysId> node=<n> cpus=<cpulist> neighbors=<a,b,..>"
  std::string describe() const;

 private:
  std::vector<CpuCluster> clusters_;
  std::vector<int> cpuToCluster_;
};

// Parses a Linux cpulist such as "0-3,8,10-11". Throws std::invalid_argument
// on malformed input.
std::vector<int> parseCpuList(folly::StringPiece s);

// Formats ascending CPUs as a cpulist, e.g. "0-3,8".
std::string formatCpuList(const std::vector<int>& cpus);

// Home cluster of the calling thread, -1 when unset. Set by
// ClusterAffinityThreadFactory for the threads it creates.
int currentThreadCluster();
void setCurrentThreadCluster(int cluster);

// Cluster the calling thread works in: its home cluster when set and valid
// for `topo`, else the cluster of the CPU it is running on, else 0.
int callerCluster(const ClusterTopology& topo);

} // namespace folly
