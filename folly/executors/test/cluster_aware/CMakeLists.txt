# Standalone test project for the cluster-aware executor prototype.
# Builds against an installed folly (BUILD_TESTS=OFF there), e.g.:
#   cmake -S . -B build -DCMAKE_PREFIX_PATH=<folly install prefix>
cmake_minimum_required(VERSION 3.14)
project(cluster_aware_test LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# folly-config refers to the gflags_shared target, so find gflags first.
find_package(gflags CONFIG REQUIRED)
find_package(folly CONFIG REQUIRED)
find_package(GTest REQUIRED)
find_package(Threads REQUIRED)

file(GLOB TEST_SOURCES CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/*Test.cpp")
add_executable(cluster_aware_test ${TEST_SOURCES})
target_link_libraries(cluster_aware_test
  PRIVATE Folly::folly GTest::gtest_main Threads::Threads)
