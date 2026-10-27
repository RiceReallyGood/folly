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

#include <stdlib.h>

#include <filesystem>
#include <fstream>
#include <map>
#include <stdexcept>
#include <string>
#include <vector>

// Helpers that build a fake /sys/devices/system tree for topology tests.
namespace folly {
namespace test {

struct FakeCpu {
  int cpu;
  int cluster;
  int node;
};

// Inclusive range [lo, hi].
inline std::vector<int> range(int lo, int hi) {
  std::vector<int> out;
  for (int i = lo; i <= hi; ++i) {
    out.push_back(i);
  }
  return out;
}

// Formats ascending CPUs the way the kernel does, e.g. "0-3,8".
inline std::string formatCpuList(const std::vector<int>& cpus) {
  std::string out;
  for (size_t i = 0; i < cpus.size();) {
    size_t j = i;
    while (j + 1 < cpus.size() && cpus[j + 1] == cpus[j] + 1) {
      ++j;
    }
    if (!out.empty()) {
      out += ',';
    }
    out += std::to_string(cpus[i]);
    if (j > i) {
      out += '-' + std::to_string(cpus[j]);
    }
    i = j + 1;
  }
  return out;
}

inline void writeFakeFile(
    const std::filesystem::path& path, const std::string& content) {
  std::filesystem::create_directories(path.parent_path());
  std::ofstream(path) << content << "\n";
}

// Writes cpu/cpuN/topology/cluster_id, cpu/cpuN/cache/index3/shared_cpu_list,
// node/nodeK/cpulist and node/nodeK/distance under a fresh temp directory and
// returns its path.
inline std::string makeFakeSysfs(
    const std::vector<FakeCpu>& cpus,
    const std::vector<std::vector<int>>& nodeDistances,
    bool withClusterId = true,
    bool withL3 = true) {
  std::string tmpl =
      (std::filesystem::temp_directory_path() / "fake_sysfs_XXXXXX").string();
  if (!mkdtemp(tmpl.data())) {
    throw std::runtime_error("mkdtemp failed");
  }
  std::filesystem::path root(tmpl);

  std::map<int, std::vector<int>> clusterCpus;
  std::map<int, std::vector<int>> nodeCpus;
  for (const auto& c : cpus) {
    clusterCpus[c.cluster].push_back(c.cpu);
    nodeCpus[c.node].push_back(c.cpu);
  }
  for (const auto& c : cpus) {
    auto dir = root / "cpu" / ("cpu" + std::to_string(c.cpu));
    if (withClusterId) {
      writeFakeFile(dir / "topology" / "cluster_id", std::to_string(c.cluster));
    }
    if (withL3) {
      writeFakeFile(
          dir / "cache" / "index3" / "shared_cpu_list",
          formatCpuList(clusterCpus[c.cluster]));
    }
  }
  for (const auto& [node, list] : nodeCpus) {
    auto dir = root / "node" / ("node" + std::to_string(node));
    writeFakeFile(dir / "cpulist", formatCpuList(list));
    std::string dist;
    for (int d : nodeDistances.at(node)) {
      dist += (dist.empty() ? "" : " ") + std::to_string(d);
    }
    writeFakeFile(dir / "distance", dist);
  }
  return root.string();
}

// 2 NUMA nodes x 2 clusters x 4 CPUs: 0-3 c0/n0, 4-7 c1/n0, 8-11 c2/n1,
// 12-15 c3/n1. Use with distances {{10, 15}, {15, 10}}.
inline std::vector<FakeCpu> fourClusterLayout() {
  std::vector<FakeCpu> out;
  for (int cpu = 0; cpu < 16; ++cpu) {
    out.push_back({cpu, cpu / 4, cpu / 8});
  }
  return out;
}

inline const std::vector<std::vector<int>>& fourClusterDistances() {
  static const std::vector<std::vector<int>> d{{10, 15}, {15, 10}};
  return d;
}

} // namespace test
} // namespace folly
