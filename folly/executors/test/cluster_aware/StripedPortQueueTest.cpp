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

// SmokeTest ported from facebook/folly 41068ce38
// folly/executors/task_queue/test/StripedPriorityUnboundedBlockingQueueTest.cpp
// for the folly::striped_port copy; the LLCAccessSpreader tests are new.

#include <folly/executors/striped_port/StripedPriorityUnboundedBlockingQueue.h>

#include <sched.h>

#include <set>
#include <string>
#include <vector>

#include <atomic>

#include <folly/Random.h>
#include <folly/FileUtil.h>
#include <folly/String.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/executors/striped_port/LLCAccessSpreader.h>
#include <folly/system/HardwareConcurrency.h>
#include <gtest/gtest.h>

namespace {

// Ensure that we test on a large number of stripes independent of the hardware
// the test runs on.
struct RandomStripe {
  static const RandomStripe& get() {
    static RandomStripe instance;
    return instance;
  }

  size_t current() const { return folly::Random::rand64(numStripes()); }
  size_t numStripes() const { return 16; }
};

} // namespace

TEST(StripedPortQueue, SmokeTest) {
  const size_t kNumThreads = folly::hardware_concurrency();
  const size_t kNumTasksPerThread = 1024;

  folly::CPUThreadPoolExecutor consumers(
      std::make_pair(kNumThreads, kNumThreads),
      folly::striped_port::StripedPriorityUnboundedBlockingQueue<
          folly::CPUThreadPoolExecutor::CPUTask,
          RandomStripe>::create(2));

  folly::CPUThreadPoolExecutor producers(
      std::make_pair(kNumThreads, kNumThreads));

  std::atomic<size_t> tasksDone{0};
  for (size_t t = 0; t < kNumThreads; ++t) {
    producers.add([&] {
      for (size_t i = 0; i < kNumTasksPerThread; ++i) {
        consumers.addWithPriority(
            [&] { ++tasksDone; }, folly::Random::rand32(2));
      }
    });
  }

  producers.join();
  consumers.join();

  EXPECT_EQ(tasksDone.load(), kNumTasksPerThread * kNumThreads);
}

namespace {

std::vector<int> allowedCpus() {
  cpu_set_t set;
  CPU_ZERO(&set);
  EXPECT_EQ(sched_getaffinity(0, sizeof(set), &set), 0);
  std::vector<int> cpus;
  for (int cpu = 0; cpu < CPU_SETSIZE; ++cpu) {
    if (CPU_ISSET(cpu, &set)) {
      cpus.push_back(cpu);
    }
  }
  return cpus;
}

// The CPUs sharing `cpu`'s L3 (index3 is the last level on the machines this
// runs on), as the kernel lists them.
std::string l3Group(int cpu) {
  std::string list;
  folly::readFile(
      ("/sys/devices/system/cpu/cpu" + std::to_string(cpu) +
       "/cache/index3/shared_cpu_list")
          .c_str(),
      list);
  return folly::rtrimWhitespace(list).str();
}

void pinTo(int cpu) {
  cpu_set_t set;
  CPU_ZERO(&set);
  CPU_SET(cpu, &set);
  ASSERT_EQ(sched_setaffinity(0, sizeof(set), &set), 0);
}

// current() caches the stripe for 16 calls; the 17th sees the new CPU.
size_t stripeAfterMigration() {
  size_t stripe = 0;
  for (int i = 0; i < 17; ++i) {
    stripe = folly::striped_port::LLCAccessSpreader::get().current();
  }
  return stripe;
}

} // namespace

TEST(StripedPortLLCAccessSpreader, NumStripesIsDistinctL3Groups) {
  std::set<std::string> groups;
  for (int cpu : allowedCpus()) {
    groups.insert(l3Group(cpu));
  }
  EXPECT_EQ(
      folly::striped_port::LLCAccessSpreader::get().numStripes(),
      groups.size());
}

TEST(StripedPortLLCAccessSpreader, CurrentFollowsPinnedCpu) {
  auto cpus = allowedCpus();
  ASSERT_FALSE(cpus.empty());
  const int a = cpus.front();
  int b = -1;
  int c = -1;
  for (int cpu : cpus) {
    if (cpu == a) {
      continue;
    }
    if (l3Group(cpu) == l3Group(a)) {
      b = b < 0 ? cpu : b;
    } else {
      c = c < 0 ? cpu : c;
    }
  }
  if (b < 0 || c < 0) {
    GTEST_SKIP() << "needs two CPUs sharing an L3 and one that does not";
  }
  cpu_set_t saved;
  ASSERT_EQ(sched_getaffinity(0, sizeof(saved), &saved), 0);
  pinTo(a);
  auto onA = stripeAfterMigration();
  pinTo(b);
  auto onB = stripeAfterMigration();
  pinTo(c);
  auto onC = stripeAfterMigration();
  ASSERT_EQ(sched_setaffinity(0, sizeof(saved), &saved), 0);
  EXPECT_EQ(onA, onB);
  EXPECT_NE(onA, onC);
}
