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

// Loop callbacks scheduled from an internal event handler (the EventBase's
// own runInEventBaseThread queue, or an EventBaseAtomicNotificationQueue
// started with startConsumingInternal, e.g. fbthrift's reply queue) must run
// before the loop blocks again. With libevent 2.x, EVLOOP_ONCE does not return
// after processing only internal events, so without a fix such callbacks wait
// for the next external event.

#include <folly/io/async/EventBase.h>
#include <folly/io/async/EventBaseAtomicNotificationQueue.h>
#include <folly/io/async/EventHandler.h>
#include <folly/synchronization/Baton.h>

#include <unistd.h>

#include <chrono>
#include <memory>
#include <thread>

#include <gtest/gtest.h>

using namespace std::chrono_literals;

namespace {

// A registered (non-internal) event that stays idle, like an idle socket on a
// server; without one libevent returns early because it has nothing to wait on.
class IdleHandler : public folly::EventHandler {
 public:
  IdleHandler(folly::EventBase* evb, int fd)
      : EventHandler(evb, folly::NetworkSocket::fromFd(fd)), fd_(fd) {}
  void handlerReady(uint16_t) noexcept override {
    char c;
    (void)!::read(fd_, &c, 1);
  }

 private:
  int fd_;
};

class LoopThread {
 public:
  LoopThread() {
    PCHECK(::pipe(pipe_) == 0);
    idle_ = std::make_unique<IdleHandler>(&evb, pipe_[0]);
    thread_ = std::thread([this] { evb.loopForever(); });
    evb.waitUntilRunning();
    evb.runInEventBaseThreadAndWait(
        [this] {
          idle_->registerHandler(
              folly::EventHandler::READ | folly::EventHandler::PERSIST);
        });
  }
  ~LoopThread() {
    evb.runInEventBaseThreadAndWait([this] { idle_->unregisterHandler(); });
    evb.terminateLoopSoon();
    thread_.join();
    idle_.reset();
    ::close(pipe_[0]);
    ::close(pipe_[1]);
  }
  // Produces an external event so a stuck loop wakes up and the test ends.
  void kick() { (void)!::write(pipe_[1], "x", 1); }
  folly::EventBase evb;

 private:
  int pipe_[2];
  std::unique_ptr<IdleHandler> idle_;
  std::thread thread_;
};

struct Msg {
  folly::Baton<>* done;
};

struct ScheduleLoopCallback {
  folly::EventBase* evb;
  void operator()(Msg&& m) noexcept {
    auto* done = m.done;
    evb->runInLoop([done] { done->post(); });
  }
};

} // namespace

TEST(EventBaseLoopCallbackWakeup, FromRunInEventBaseThread) {
  LoopThread t;
  std::this_thread::sleep_for(50ms); // let the loop go idle
  folly::Baton<> done;
  t.evb.runInEventBaseThread(
      [&] { t.evb.runInLoop([&] { done.post(); }); });
  EXPECT_TRUE(done.try_wait_for(1s));
  if (!done.ready()) {
    t.kick();
  }
  done.wait();
}

TEST(EventBaseLoopCallbackWakeup, FromInternalNotificationQueueConsumer) {
  LoopThread t;
  folly::EventBaseAtomicNotificationQueue<Msg, ScheduleLoopCallback> q(
      ScheduleLoopCallback{&t.evb});
  q.setMaxReadAtOnce(0);
  t.evb.runInEventBaseThreadAndWait([&] { q.startConsumingInternal(&t.evb); });
  for (int i = 0; i < 20; ++i) {
    std::this_thread::sleep_for(5ms); // let the loop go idle
    folly::Baton<> done;
    q.putMessage(Msg{&done});
    EXPECT_TRUE(done.try_wait_for(1s)) << "iteration " << i;
    if (!done.ready()) {
      t.kick();
    }
    done.wait();
  }
  t.evb.runInEventBaseThreadAndWait([&] { q.stopConsuming(); });
}
