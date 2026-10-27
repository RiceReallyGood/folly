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

#include <folly/executors/ClusterTopology.h>

#include <sched.h>

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <map>
#include <sstream>
#include <stdexcept>
#include <utility>

#include <folly/Conv.h>
#include <folly/Indestructible.h>
#include <folly/Optional.h>
#include <folly/String.h>

namespace folly {

namespace {

thread_local int tlsCluster = -1;

folly::Optional<std::string> readTrimmed(const std::filesystem::path& path) {
  std::ifstream in(path);
  if (!in) {
    return folly::none;
  }
  std::stringstream ss;
  ss << in.rdbuf();
  return folly::trimWhitespace(ss.str()).str();
}

std::vector<int> affinityCpus() {
  std::vector<int> cpus;
  cpu_set_t set;
  CPU_ZERO(&set);
  if (sched_getaffinity(0, sizeof(set), &set) == 0) {
    for (int cpu = 0; cpu < CPU_SETSIZE; ++cpu) {
      if (CPU_ISSET(cpu, &set)) {
        cpus.push_back(cpu);
      }
    }
  }
  return cpus;
}

// Grouping key of one CPU: {0, cluster_id}, {1, lowest CPU sharing its L3},
// or {2, 0} when sysfs has neither.
std::pair<int, int> clusterKey(const std::filesystem::path& cpuDir) {
  if (auto id = readTrimmed(cpuDir / "topology" / "cluster_id")) {
    if (auto v = folly::tryTo<int>(*id)) {
      return {0, *v};
    }
  }
  if (auto l3 = readTrimmed(cpuDir / "cache" / "index3" / "shared_cpu_list")) {
    try {
      auto cpus = parseCpuList(*l3);
      if (!cpus.empty()) {
        return {1, *std::min_element(cpus.begin(), cpus.end())};
      }
    } catch (const std::invalid_argument&) {
    }
  }
  return {2, 0};
}

} // namespace

std::vector<int> parseCpuList(folly::StringPiece s) {
  std::vector<int> out;
  s = folly::trimWhitespace(s);
  if (s.empty()) {
    return out;
  }
  std::vector<folly::StringPiece> parts;
  folly::split(',', s, parts);
  for (auto part : parts) {
    part = folly::trimWhitespace(part);
    auto lo = part;
    auto hi = part;
    auto dash = part.find('-');
    if (dash != folly::StringPiece::npos) {
      lo = part.subpiece(0, dash);
      hi = part.subpiece(dash + 1);
    }
    auto a = folly::tryTo<int>(lo);
    auto b = folly::tryTo<int>(hi);
    if (!a || !b || *a < 0 || *a > *b) {
      throw std::invalid_argument("bad cpulist: " + s.str());
    }
    for (int cpu = *a; cpu <= *b; ++cpu) {
      out.push_back(cpu);
    }
  }
  return out;
}

std::string formatCpuList(const std::vector<int>& cpus) {
  std::string out;
  for (size_t i = 0; i < cpus.size();) {
    size_t j = i;
    while (j + 1 < cpus.size() && cpus[j + 1] == cpus[j] + 1) {
      ++j;
    }
    if (!out.empty()) {
      out += ',';
    }
    out += folly::to<std::string>(cpus[i]);
    if (j > i) {
      out += '-' + folly::to<std::string>(cpus[j]);
    }
    i = j + 1;
  }
  return out;
}

ClusterTopology ClusterTopology::detect(
    const std::string& sysRoot, std::vector<int> allowedCpus) {
  if (allowedCpus.empty()) {
    allowedCpus = affinityCpus();
  }
  std::sort(allowedCpus.begin(), allowedCpus.end());
  allowedCpus.erase(
      std::unique(allowedCpus.begin(), allowedCpus.end()), allowedCpus.end());

  const std::filesystem::path root(sysRoot);
  std::map<int, int> cpuNode;
  std::map<int, std::vector<int>> nodeDistance;
  std::error_code ec;
  for (const auto& entry :
       std::filesystem::directory_iterator(root / "node", ec)) {
    auto name = entry.path().filename().string();
    if (name.rfind("node", 0) != 0) {
      continue;
    }
    auto node = folly::tryTo<int>(folly::StringPiece(name).subpiece(4));
    if (!node) {
      continue;
    }
    if (auto list = readTrimmed(entry.path() / "cpulist")) {
      try {
        for (int cpu : parseCpuList(*list)) {
          cpuNode[cpu] = *node;
        }
      } catch (const std::invalid_argument&) {
      }
    }
    if (auto dist = readTrimmed(entry.path() / "distance")) {
      std::vector<folly::StringPiece> parts;
      folly::split(' ', *dist, parts, /*ignoreEmpty=*/true);
      std::vector<int> row;
      for (auto p : parts) {
        row.push_back(folly::tryTo<int>(p).value_or(20));
      }
      nodeDistance[*node] = std::move(row);
    }
  }

  std::map<std::pair<int, int>, std::vector<int>> groups;
  for (int cpu : allowedCpus) {
    groups[clusterKey(root / "cpu" / ("cpu" + folly::to<std::string>(cpu)))]
        .push_back(cpu);
  }

  ClusterTopology topo;
  for (auto& [key, cpus] : groups) {
    auto node = cpuNode.find(cpus.front());
    topo.clusters_.push_back(CpuCluster{
        0,
        key.second,
        node == cpuNode.end() ? 0 : node->second,
        std::move(cpus),
        {}});
  }
  std::sort(
      topo.clusters_.begin(),
      topo.clusters_.end(),
      [](const CpuCluster& a, const CpuCluster& b) {
        return a.cpus.front() < b.cpus.front();
      });

  auto distance = [&](int a, int b) {
    auto row = nodeDistance.find(a);
    if (row != nodeDistance.end() && b >= 0 &&
        static_cast<size_t>(b) < row->second.size()) {
      return row->second[b];
    }
    return a == b ? 10 : 20;
  };
  int maxCpu = allowedCpus.empty() ? -1 : allowedCpus.back();
  topo.cpuToCluster_.assign(maxCpu + 1, -1);
  for (size_t i = 0; i < topo.clusters_.size(); ++i) {
    auto& c = topo.clusters_[i];
    c.id = static_cast<int>(i);
    for (int cpu : c.cpus) {
      topo.cpuToCluster_[cpu] = c.id;
    }
  }
  for (auto& c : topo.clusters_) {
    for (const auto& other : topo.clusters_) {
      if (other.id != c.id) {
        c.neighbors.push_back(other.id);
      }
    }
    std::stable_sort(
        c.neighbors.begin(), c.neighbors.end(), [&](int a, int b) {
          return distance(c.numaNode, topo.clusters_[a].numaNode) <
              distance(c.numaNode, topo.clusters_[b].numaNode);
        });
  }
  return topo;
}

const ClusterTopology& ClusterTopology::instance() {
  // Never destroyed: pool threads (e.g. ClusterAwareBlockingQueue workers) may
  // still use it while static destructors run at exit.
  static const Indestructible<ClusterTopology> topo{
      detect("/sys/devices/system", {})};
  return *topo;
}

int ClusterTopology::clusterOfCpu(int cpu) const {
  if (cpu < 0 || static_cast<size_t>(cpu) >= cpuToCluster_.size()) {
    return -1;
  }
  return cpuToCluster_[cpu];
}

std::string ClusterTopology::describe() const {
  std::string out;
  for (const auto& c : clusters_) {
    std::string neighbors;
    for (int n : c.neighbors) {
      neighbors += (neighbors.empty() ? "" : ",") + folly::to<std::string>(n);
    }
    out += folly::to<std::string>(
        "cluster ",
        c.id,
        " sys=",
        c.sysClusterId,
        " node=",
        c.numaNode,
        " cpus=",
        formatCpuList(c.cpus),
        " neighbors=",
        neighbors,
        "\n");
  }
  return out;
}

int currentThreadCluster() {
  return tlsCluster;
}

void setCurrentThreadCluster(int cluster) {
  tlsCluster = cluster;
}

int callerCluster(const ClusterTopology& topo) {
  if (topo.size() <= 1) {
    return 0;
  }
  int home = tlsCluster;
  if (home >= 0 && static_cast<size_t>(home) < topo.size()) {
    return home;
  }
  int cluster = topo.clusterOfCpu(sched_getcpu());
  return cluster >= 0 ? cluster : 0;
}

} // namespace folly
