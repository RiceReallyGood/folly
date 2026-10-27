# Standalone test for EventBase loop-callback wakeups. Builds against an
# installed folly (BUILD_TESTS=OFF there), e.g.:
#   cmake -S . -B build -DCMAKE_PREFIX_PATH=<folly install prefix>
cmake_minimum_required(VERSION 3.14)
project(evb_wakeup_test LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# folly-config refers to the gflags_shared target, so find gflags first.
find_package(gflags CONFIG REQUIRED)
find_package(folly CONFIG REQUIRED)
find_package(GTest REQUIRED)
find_package(Threads REQUIRED)

add_executable(evb_wakeup_test EventBaseLoopCallbackWakeupTest.cpp)
target_link_libraries(evb_wakeup_test
  PRIVATE Folly::folly GTest::gtest_main Threads::Threads)
