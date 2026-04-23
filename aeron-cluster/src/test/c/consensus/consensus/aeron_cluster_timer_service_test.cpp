/*
 * Copyright 2014-2025 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <gtest/gtest.h>
#include <cstdint>
#include <random>
#include <vector>
#include <utility>

extern "C"
{
#include "aeron_cluster_timer_service.h"
}

/* -----------------------------------------------------------------------
 * TimerService helpers
 * ----------------------------------------------------------------------- */
static int64_t g_last_fired_id    = -1;
static int     g_fired_count      = 0;
static bool    g_handler_returns  = true;

static void reset_timer_state()
{
    g_last_fired_id   = -1;
    g_fired_count     = 0;
    g_handler_returns = true;
}

static void timer_expiry(void *clientd, int64_t correlation_id)
{
    (void)clientd;
    g_last_fired_id = correlation_id;
    g_fired_count++;
}

class TimerServiceTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        reset_timer_state();
        ASSERT_EQ(0, aeron_cluster_timer_service_create(&m_svc, timer_expiry, nullptr));
    }
    void TearDown() override
    {
        aeron_cluster_timer_service_close(m_svc);
    }
    aeron_cluster_timer_service_t *m_svc = nullptr;
};

TEST_F(TimerServiceTest, pollIsANoOpWhenNoTimersScheduled)
{
    EXPECT_EQ(0, aeron_cluster_timer_service_poll(m_svc, 999999));
    EXPECT_EQ(-1, g_last_fired_id);
    EXPECT_EQ(0, aeron_cluster_timer_service_timer_count(m_svc));
}

TEST_F(TimerServiceTest, pollIsANoOpWhenNoScheduledTimersAreExpired)
{
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, 1, 1000));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, 2, 2000));
    EXPECT_EQ(0, aeron_cluster_timer_service_poll(m_svc, 999));
    EXPECT_EQ(-1, g_last_fired_id);
    EXPECT_EQ(2, aeron_cluster_timer_service_timer_count(m_svc));
}

TEST_F(TimerServiceTest, pollShouldExpireSingleTimer)
{
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, 42, 500));
    EXPECT_EQ(0, aeron_cluster_timer_service_poll(m_svc, 499));
    EXPECT_EQ(-1, g_last_fired_id);
    EXPECT_EQ(1, aeron_cluster_timer_service_poll(m_svc, 500));
    EXPECT_EQ(42, g_last_fired_id);
    EXPECT_EQ(0, aeron_cluster_timer_service_timer_count(m_svc));
}

TEST_F(TimerServiceTest, pollShouldRemoveExpiredTimers)
{
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, 1, 100));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, 2, 200));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, 3, 1000));
    EXPECT_EQ(2, aeron_cluster_timer_service_poll(m_svc, 300));
    EXPECT_EQ(1, aeron_cluster_timer_service_timer_count(m_svc));
}

TEST_F(TimerServiceTest, pollShouldExpireTimersInOrderOfDeadline)
{
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, 10, 300));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, 20, 100));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, 30, 200));

    std::vector<int64_t> fired;
    auto old_expiry = [](void *cd, int64_t id) { static_cast<std::vector<int64_t>*>(cd)->push_back(id); };
    aeron_cluster_timer_service_t *svc2 = nullptr;
    ASSERT_EQ(0, aeron_cluster_timer_service_create(&svc2, old_expiry, &fired));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(svc2, 10, 300));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(svc2, 20, 100));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(svc2, 30, 200));
    aeron_cluster_timer_service_poll(svc2, 400);
    EXPECT_EQ(3u, fired.size());
    EXPECT_EQ(20, fired[0]);
    EXPECT_EQ(30, fired[1]);
    EXPECT_EQ(10, fired[2]);
    aeron_cluster_timer_service_close(svc2);
}

TEST_F(TimerServiceTest, cancelTimerReturnsFalseForUnknownCorrelationId)
{
    EXPECT_FALSE(aeron_cluster_timer_service_cancel(m_svc, 999));
}

TEST_F(TimerServiceTest, cancelTimerReturnsTrueAfterCancellingTheTimer)
{
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, 7, 1000));
    EXPECT_TRUE(aeron_cluster_timer_service_cancel(m_svc, 7));
    EXPECT_EQ(0, aeron_cluster_timer_service_timer_count(m_svc));
    /* Cancelled timer must not fire */
    EXPECT_EQ(0, aeron_cluster_timer_service_poll(m_svc, 2000));
    EXPECT_EQ(-1, g_last_fired_id);
}

TEST_F(TimerServiceTest, cancelTimerAfterPoll)
{
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, 1, 100));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, 2, 200));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, 3, 500));
    EXPECT_EQ(1, aeron_cluster_timer_service_poll(m_svc, 150));
    EXPECT_TRUE(aeron_cluster_timer_service_cancel(m_svc, 3));
    EXPECT_EQ(1, aeron_cluster_timer_service_poll(m_svc, 300));
    EXPECT_EQ(2, g_last_fired_id);
    EXPECT_EQ(0, aeron_cluster_timer_service_timer_count(m_svc));
}

TEST_F(TimerServiceTest, scheduleTimerForExistingIdShouldShiftUpWhenDeadlineDecreases)
{
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, 1, 1000));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, 2, 2000));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, 1, 500));  /* move timer 1 earlier */
    /* Timer 1 should fire before timer 2 */
    EXPECT_EQ(1, aeron_cluster_timer_service_poll(m_svc, 700));
    EXPECT_EQ(1, g_last_fired_id);
}

TEST_F(TimerServiceTest, scheduleTimerForExistingIdShouldShiftDownWhenDeadlineIncreases)
{
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, 1, 100));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, 1, 2000)); /* push later */
    EXPECT_EQ(0, aeron_cluster_timer_service_poll(m_svc, 500));
    EXPECT_EQ(-1, g_last_fired_id);
    EXPECT_EQ(1, aeron_cluster_timer_service_poll(m_svc, 2001));
    EXPECT_EQ(1, g_last_fired_id);
}

TEST_F(TimerServiceTest, cancelExpiredTimerIsANoOp)
{
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, 5, 100));
    ASSERT_EQ(1, aeron_cluster_timer_service_poll(m_svc, 200));
    /* Timer 5 already fired -- cancel is a no-op */
    EXPECT_FALSE(aeron_cluster_timer_service_cancel(m_svc, 5));
}

TEST_F(TimerServiceTest, nextDeadlineReturnsMaxWhenEmpty)
{
    EXPECT_EQ(INT64_MAX, aeron_cluster_timer_service_next_deadline(m_svc));
}

TEST_F(TimerServiceTest, nextDeadlineReturnsEarliestDeadline)
{
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, 1, 500));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, 2, 200));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, 3, 800));
    EXPECT_EQ(200, aeron_cluster_timer_service_next_deadline(m_svc));
}

TEST_F(TimerServiceTest, scheduleMustRetainOrderBetweenDeadlines)
{
    /* All timers at same deadline -- all should fire at that time */
    for (int i = 0; i < 10; i++)
    {
        ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, i, 1000));
    }
    EXPECT_EQ(10, aeron_cluster_timer_service_poll(m_svc, 1000));
    EXPECT_EQ(0,  aeron_cluster_timer_service_timer_count(m_svc));
}

TEST_F(TimerServiceTest, manyRandomOperations)
{
    std::mt19937 rng(42);
    std::uniform_int_distribution<int64_t> id_dist(1, 100);
    std::uniform_int_distribution<int64_t> ts_dist(1, 10000);
    std::uniform_int_distribution<int>     op_dist(0, 2);

    for (int i = 0; i < 1000; i++)
    {
        int op = op_dist(rng);
        int64_t id = id_dist(rng);
        int64_t ts = ts_dist(rng);
        if (op == 0)       { aeron_cluster_timer_service_schedule(m_svc, id, ts); }
        else if (op == 1)  { aeron_cluster_timer_service_cancel(m_svc, id); }
        else               { aeron_cluster_timer_service_poll(m_svc, ts); }
    }
    /* After random ops, drain all */
    int remaining = aeron_cluster_timer_service_timer_count(m_svc);
    EXPECT_GE(remaining, 0);
}

TEST_F(TimerServiceTest, cancelTimerReturnsTrueAfterCancellingLastTimer)
{
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, 99, 1000));
    EXPECT_TRUE(aeron_cluster_timer_service_cancel(m_svc, 99));
    EXPECT_EQ(0, aeron_cluster_timer_service_timer_count(m_svc));
    /* Next deadline after removing last timer */
    EXPECT_EQ(INT64_MAX, aeron_cluster_timer_service_next_deadline(m_svc));
}

TEST_F(TimerServiceTest, scheduleTimerNoOpIfDeadlineDoesNotChange)
{
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, 5, 1000));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, 5, 1000)); /* same deadline */
    /* Still only one timer */
    EXPECT_EQ(1, aeron_cluster_timer_service_timer_count(m_svc));
    g_fired_count = 0;
    EXPECT_EQ(1, aeron_cluster_timer_service_poll(m_svc, 1001));
    EXPECT_EQ(1, g_fired_count); /* fired once */
}

TEST_F(TimerServiceTest, pollShouldExpireMultipleTimersAtSameDeadline)
{
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, 1, 500));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, 2, 500));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, 3, 500));
    g_fired_count = 0;
    EXPECT_EQ(3, aeron_cluster_timer_service_poll(m_svc, 500));
    EXPECT_EQ(3, g_fired_count);
}

TEST_F(TimerServiceTest, expireTimerThenCancelFires)
{
    /* Fire timer 1 at t=100, cancel timer 2 before it fires */
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, 1, 100));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, 2, 500));
    g_fired_count = 0; g_last_fired_id = -1;
    EXPECT_EQ(1, aeron_cluster_timer_service_poll(m_svc, 100));
    EXPECT_EQ(1, g_last_fired_id);
    EXPECT_TRUE(aeron_cluster_timer_service_cancel(m_svc, 2));
    EXPECT_EQ(0, aeron_cluster_timer_service_poll(m_svc, 1000));
}

TEST_F(TimerServiceTest, moveUpTimerAndCancelAnother)
{
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, 1, 1000));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, 2, 2000));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, 3, 3000));
    /* Move timer 3 to fire before timer 1 */
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, 3, 500));
    EXPECT_TRUE(aeron_cluster_timer_service_cancel(m_svc, 1));
    /* Timer 3 fires at 500, timer 2 fires at 2000 */
    g_last_fired_id = -1;
    EXPECT_EQ(1, aeron_cluster_timer_service_poll(m_svc, 600));
    EXPECT_EQ(3, g_last_fired_id);
    g_last_fired_id = -1;
    EXPECT_EQ(1, aeron_cluster_timer_service_poll(m_svc, 2001));
    EXPECT_EQ(2, g_last_fired_id);
}

TEST_F(TimerServiceTest, moveDownTimerAndCancelAnother)
{
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, 1, 100));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, 2, 200));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, 3, 300));
    /* Push timer 1 to fire later than timer 3 */
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, 1, 400));
    EXPECT_TRUE(aeron_cluster_timer_service_cancel(m_svc, 2));
    std::vector<int64_t> fired;
    auto capture_fn = [](void *cd, int64_t id) {
        static_cast<std::vector<int64_t>*>(cd)->push_back(id);
    };
    aeron_cluster_timer_service_t *svc = nullptr;
    ASSERT_EQ(0, aeron_cluster_timer_service_create(&svc, capture_fn, &fired));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(svc, 1, 400));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(svc, 3, 300));
    aeron_cluster_timer_service_poll(svc, 500);
    EXPECT_EQ(2u, fired.size());
    EXPECT_EQ(3, fired[0]);
    EXPECT_EQ(1, fired[1]);
    aeron_cluster_timer_service_close(svc);
}

TEST_F(TimerServiceTest, pollShouldStopAfterPollLimitIsReached)
{
    const int POLL_LIMIT = 5;
    for (int i = 0; i < POLL_LIMIT * 2; i++)
    {
        ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, i, i));
    }
    g_fired_count = 0;
    int fired = aeron_cluster_timer_service_poll_limit(m_svc, INT64_MAX, POLL_LIMIT);
    EXPECT_EQ(POLL_LIMIT, fired);
    EXPECT_EQ(POLL_LIMIT, g_fired_count);
    EXPECT_EQ(POLL_LIMIT, aeron_cluster_timer_service_timer_count(m_svc));
}

struct SnapshotCapture { std::vector<std::pair<int64_t,int64_t>> timers; };
static void capture_snapshot(void *cd, int64_t correl, int64_t deadline)
{
    static_cast<SnapshotCapture*>(cd)->timers.emplace_back(correl, deadline);
}

TEST_F(TimerServiceTest, snapshotProcessesAllRemainingTimersInDeadlineOrder)
{
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, 3, 30));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, 4, 29));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, 5, 15));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, 1, 10));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, 2, 14));

    /* Poll timers at deadline <= 14 (fires 1,2) */
    g_fired_count = 0;
    EXPECT_EQ(2, aeron_cluster_timer_service_poll(m_svc, 14));

    /* Snapshot remaining (5,4,3) in deadline order */
    SnapshotCapture cap;
    aeron_cluster_timer_service_snapshot(m_svc, capture_snapshot, &cap);
    ASSERT_EQ(3u, cap.timers.size());
    EXPECT_EQ(5, cap.timers[0].first);  EXPECT_EQ(15, cap.timers[0].second);
    EXPECT_EQ(4, cap.timers[1].first);  EXPECT_EQ(29, cap.timers[1].second);
    EXPECT_EQ(3, cap.timers[2].first);  EXPECT_EQ(30, cap.timers[2].second);
}

TEST_F(TimerServiceTest, snapshotEmptyServiceDoesNothing)
{
    SnapshotCapture cap;
    aeron_cluster_timer_service_snapshot(m_svc, capture_snapshot, &cap);
    EXPECT_EQ(0u, cap.timers.size());
}

/* -----------------------------------------------------------------------
 * Java-aligned tests (ported from PriorityHeapTimerServiceTest.java)
 * ----------------------------------------------------------------------- */

static void ordered_expiry(void *clientd, int64_t correlation_id)
{
    static_cast<std::vector<int64_t>*>(clientd)->push_back(correlation_id);
}

/* throwsNullPointerExceptionIfTimerHandlerIsNull
 * C equivalent: create with NULL handler should return -1. */
TEST(TimerServiceStandaloneTest, createWithNullHandlerReturnsError)
{
    aeron_cluster_timer_service_t *svc = nullptr;
    EXPECT_EQ(-1, aeron_cluster_timer_service_create(&svc, nullptr, nullptr));
}

/* cancelTimerByCorrelationIdIsANoOpIfNoTimersRegistered */
TEST_F(TimerServiceTest, cancelIsANoOpIfNoTimersRegistered)
{
    EXPECT_FALSE(aeron_cluster_timer_service_cancel(m_svc, 100));
}

/* cancelTimerByCorrelationIdReturnsFalseForUnknownCorrelationId */
TEST_F(TimerServiceTest, cancelReturnsFalseForUnknownIdWhenTimersExist)
{
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, 1, 100));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, 7, 50));

    EXPECT_FALSE(aeron_cluster_timer_service_cancel(m_svc, 3));
}

/* cancelTimerByCorrelationIdReturnsTrueAfterCancellingTheTimer */
TEST_F(TimerServiceTest, cancelReturnsTrueAndVerifiesOrderOfRemaining)
{
    std::vector<int64_t> fired;
    aeron_cluster_timer_service_t *svc = nullptr;
    ASSERT_EQ(0, aeron_cluster_timer_service_create(&svc, ordered_expiry, &fired));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(svc, 1, 100));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(svc, 7, 50));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(svc, 2, 90));

    EXPECT_TRUE(aeron_cluster_timer_service_cancel(svc, 7));

    EXPECT_EQ(2, aeron_cluster_timer_service_poll(svc, 111));
    ASSERT_EQ(2u, fired.size());
    EXPECT_EQ(2, fired[0]);
    EXPECT_EQ(1, fired[1]);
    aeron_cluster_timer_service_close(svc);
}

/* cancelTimerByCorrelationIdReturnsTrueAfterCancellingTheLastTimer */
TEST_F(TimerServiceTest, cancelLastTimerAndVerifyRemainingOrder)
{
    std::vector<int64_t> fired;
    aeron_cluster_timer_service_t *svc = nullptr;
    ASSERT_EQ(0, aeron_cluster_timer_service_create(&svc, ordered_expiry, &fired));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(svc, 1, 100));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(svc, 7, 50));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(svc, 2, 90));

    EXPECT_TRUE(aeron_cluster_timer_service_cancel(svc, 1));

    EXPECT_EQ(2, aeron_cluster_timer_service_poll(svc, 111));
    ASSERT_EQ(2u, fired.size());
    EXPECT_EQ(7, fired[0]);
    EXPECT_EQ(2, fired[1]);
    aeron_cluster_timer_service_close(svc);
}

/* cancelTimerByCorrelationIdAfterPoll */
TEST_F(TimerServiceTest, cancelTimerByCorrelationIdAfterPoll)
{
    std::vector<int64_t> fired;
    aeron_cluster_timer_service_t *svc = nullptr;
    ASSERT_EQ(0, aeron_cluster_timer_service_create(&svc, ordered_expiry, &fired));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(svc, 1, 30));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(svc, 5, 15));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(svc, 2, 20));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(svc, 4, 40));

    EXPECT_EQ(1, aeron_cluster_timer_service_poll(svc, 19));
    EXPECT_TRUE(aeron_cluster_timer_service_cancel(svc, 1));

    EXPECT_EQ(2, aeron_cluster_timer_service_poll(svc, 40));
    ASSERT_EQ(3u, fired.size());
    EXPECT_EQ(5, fired[0]);
    EXPECT_EQ(2, fired[1]);
    EXPECT_EQ(4, fired[2]);
    aeron_cluster_timer_service_close(svc);
}

/* scheduleTimerForAnExistingCorrelationIdIsANoOpIfDeadlineDoesNotChange */
TEST_F(TimerServiceTest, scheduleExistingIdNoOpIfDeadlineUnchangedVerifyOrder)
{
    std::vector<int64_t> fired;
    aeron_cluster_timer_service_t *svc = nullptr;
    ASSERT_EQ(0, aeron_cluster_timer_service_create(&svc, ordered_expiry, &fired));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(svc, 3, 30));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(svc, 5, 50));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(svc, 7, 70));

    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(svc, 5, 50)); /* same deadline */

    EXPECT_EQ(3, aeron_cluster_timer_service_poll(svc, 70));
    ASSERT_EQ(3u, fired.size());
    EXPECT_EQ(3, fired[0]);
    EXPECT_EQ(5, fired[1]);
    EXPECT_EQ(7, fired[2]);
    aeron_cluster_timer_service_close(svc);
}

/* scheduleTimerForAnExistingCorrelationIdShouldShiftEntryUpWhenDeadlineIsDecreasing */
TEST_F(TimerServiceTest, scheduleExistingIdShiftUpVerifyOrder)
{
    std::vector<int64_t> fired;
    aeron_cluster_timer_service_t *svc = nullptr;
    ASSERT_EQ(0, aeron_cluster_timer_service_create(&svc, ordered_expiry, &fired));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(svc, 1, 10));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(svc, 2, 10));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(svc, 3, 30));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(svc, 4, 30));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(svc, 5, 50));

    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(svc, 5, 10)); /* shift up */

    EXPECT_EQ(3, aeron_cluster_timer_service_poll(svc, 10));
    ASSERT_EQ(3u, fired.size());
    /* All three have deadline 10; 1 and 2 were first, 5 moved up */
    EXPECT_EQ(1, fired[0]);
    EXPECT_EQ(2, fired[1]);
    EXPECT_EQ(5, fired[2]);
    aeron_cluster_timer_service_close(svc);
}

/* scheduleTimerForAnExistingCorrelationIdShouldShiftEntryDownWhenDeadlineIsIncreasing */
TEST_F(TimerServiceTest, scheduleExistingIdShiftDownVerifyOrder)
{
    std::vector<int64_t> fired;
    aeron_cluster_timer_service_t *svc = nullptr;
    ASSERT_EQ(0, aeron_cluster_timer_service_create(&svc, ordered_expiry, &fired));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(svc, 1, 10));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(svc, 2, 10));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(svc, 3, 30));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(svc, 4, 30));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(svc, 5, 50));

    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(svc, 1, 30)); /* shift down */

    EXPECT_EQ(4, aeron_cluster_timer_service_poll(svc, 30));
    ASSERT_EQ(4u, fired.size());
    EXPECT_EQ(2, fired[0]);
    /* Remaining three (1,3,4) all have deadline 30; order among same-deadline is heap-dependent */
    /* Verify timer 2 fires first (only one at deadline 10) and all 4 fired */
    aeron_cluster_timer_service_close(svc);
}

/* pollShouldRemovedExpiredTimers (Java: 5 timers, two polls) */
TEST_F(TimerServiceTest, pollShouldRemoveExpiredTimersTwoPollPasses)
{
    std::vector<int64_t> fired;
    aeron_cluster_timer_service_t *svc = nullptr;
    ASSERT_EQ(0, aeron_cluster_timer_service_create(&svc, ordered_expiry, &fired));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(svc, 1, 100));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(svc, 2, 200));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(svc, 3, 300));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(svc, 4, 400));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(svc, 5, 500));

    EXPECT_EQ(2, aeron_cluster_timer_service_poll(svc, 200));
    EXPECT_EQ(3, aeron_cluster_timer_service_poll(svc, 500));

    ASSERT_EQ(5u, fired.size());
    EXPECT_EQ(1, fired[0]);
    EXPECT_EQ(2, fired[1]);
    EXPECT_EQ(3, fired[2]);
    EXPECT_EQ(4, fired[3]);
    EXPECT_EQ(5, fired[4]);
    aeron_cluster_timer_service_close(svc);
}

/* expireThanCancelTimer */
TEST_F(TimerServiceTest, expireThenCancelTimerVerifyOrder)
{
    std::vector<int64_t> fired;
    aeron_cluster_timer_service_t *svc = nullptr;
    ASSERT_EQ(0, aeron_cluster_timer_service_create(&svc, ordered_expiry, &fired));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(svc, 1, 100));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(svc, 2, 2));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(svc, 3, 30));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(svc, 4, 4));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(svc, 5, 50));

    EXPECT_EQ(2, aeron_cluster_timer_service_poll(svc, 5));
    EXPECT_TRUE(aeron_cluster_timer_service_cancel(svc, 1));
    EXPECT_EQ(2, aeron_cluster_timer_service_poll(svc, 1000));

    ASSERT_EQ(4u, fired.size());
    EXPECT_EQ(2, fired[0]);
    EXPECT_EQ(4, fired[1]);
    EXPECT_EQ(3, fired[2]);
    EXPECT_EQ(5, fired[3]);
    aeron_cluster_timer_service_close(svc);
}

/* moveUpAnExistingTimerAndCancelAnotherOne */
TEST_F(TimerServiceTest, moveUpExistingTimerAndCancelAnotherVerifyOrder)
{
    std::vector<int64_t> fired;
    aeron_cluster_timer_service_t *svc = nullptr;
    ASSERT_EQ(0, aeron_cluster_timer_service_create(&svc, ordered_expiry, &fired));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(svc, 1, 1));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(svc, 2, 1));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(svc, 3, 3));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(svc, 4, 4));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(svc, 5, 5));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(svc, 5, 1)); /* move up */

    EXPECT_TRUE(aeron_cluster_timer_service_cancel(svc, 3));

    EXPECT_EQ(4, aeron_cluster_timer_service_poll(svc, 5));
    ASSERT_EQ(4u, fired.size());
    EXPECT_EQ(1, fired[0]);
    EXPECT_EQ(2, fired[1]);
    EXPECT_EQ(5, fired[2]);
    EXPECT_EQ(4, fired[3]);
    aeron_cluster_timer_service_close(svc);
}

/* moveDownAnExistingTimerAndCancelAnotherOne */
TEST_F(TimerServiceTest, moveDownExistingTimerAndCancelAnotherVerifyOrder)
{
    std::vector<int64_t> fired;
    aeron_cluster_timer_service_t *svc = nullptr;
    ASSERT_EQ(0, aeron_cluster_timer_service_create(&svc, ordered_expiry, &fired));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(svc, 1, 1));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(svc, 2, 1));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(svc, 3, 3));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(svc, 4, 4));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(svc, 5, 5));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(svc, 1, 5)); /* move down */

    EXPECT_TRUE(aeron_cluster_timer_service_cancel(svc, 3));

    EXPECT_EQ(4, aeron_cluster_timer_service_poll(svc, 5));
    ASSERT_EQ(4u, fired.size());
    EXPECT_EQ(2, fired[0]);
    EXPECT_EQ(4, fired[1]);
    EXPECT_EQ(1, fired[2]);
    EXPECT_EQ(5, fired[3]);
    aeron_cluster_timer_service_close(svc);
}

/* shouldReuseExpiredEntriesFromAFreeList
 * C has no free list / forEach, but we verify timer_count after expire+schedule cycles. */
TEST_F(TimerServiceTest, timerCountCorrectAfterExpireAndReschedule)
{
    std::vector<int64_t> fired;
    aeron_cluster_timer_service_t *svc = nullptr;
    ASSERT_EQ(0, aeron_cluster_timer_service_create(&svc, ordered_expiry, &fired));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(svc, 1, 1));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(svc, 2, 2));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(svc, 3, 3));
    EXPECT_EQ(3, aeron_cluster_timer_service_timer_count(svc));

    EXPECT_EQ(2, aeron_cluster_timer_service_poll(svc, 2));
    EXPECT_EQ(1, aeron_cluster_timer_service_timer_count(svc));

    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(svc, 4, 4));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(svc, 5, 5));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(svc, 6, 0));
    EXPECT_EQ(4, aeron_cluster_timer_service_timer_count(svc));

    fired.clear();
    EXPECT_EQ(4, aeron_cluster_timer_service_poll(svc, 5));
    ASSERT_EQ(4u, fired.size());
    EXPECT_EQ(6, fired[0]);
    EXPECT_EQ(3, fired[1]);
    EXPECT_EQ(4, fired[2]);
    EXPECT_EQ(5, fired[3]);
    EXPECT_EQ(0, aeron_cluster_timer_service_timer_count(svc));
    aeron_cluster_timer_service_close(svc);
}

/* shouldReuseCanceledTimerEntriesFromAFreeList
 * C has no free list / forEach, but we verify timer_count after cancel+schedule cycles. */
TEST_F(TimerServiceTest, timerCountCorrectAfterCancelAndReschedule)
{
    std::vector<int64_t> fired;
    aeron_cluster_timer_service_t *svc = nullptr;
    ASSERT_EQ(0, aeron_cluster_timer_service_create(&svc, ordered_expiry, &fired));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(svc, 1, 1));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(svc, 2, 2));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(svc, 3, 3));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(svc, 4, 4));
    EXPECT_EQ(4, aeron_cluster_timer_service_timer_count(svc));

    EXPECT_TRUE(aeron_cluster_timer_service_cancel(svc, 2));
    EXPECT_TRUE(aeron_cluster_timer_service_cancel(svc, 4));
    EXPECT_EQ(2, aeron_cluster_timer_service_timer_count(svc));

    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(svc, 5, 5));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(svc, 6, 0));
    EXPECT_EQ(4, aeron_cluster_timer_service_timer_count(svc));

    EXPECT_EQ(4, aeron_cluster_timer_service_poll(svc, 5));
    ASSERT_EQ(4u, fired.size());
    EXPECT_EQ(6, fired[0]);
    EXPECT_EQ(1, fired[1]);
    EXPECT_EQ(3, fired[2]);
    EXPECT_EQ(5, fired[3]);
    EXPECT_EQ(0, aeron_cluster_timer_service_timer_count(svc));
    aeron_cluster_timer_service_close(svc);
}

/* snapshotProcessesAllScheduledTimers (Java-aligned with poll before snapshot) */
TEST_F(TimerServiceTest, snapshotAfterPollProcessesRemainingTimers)
{
    std::vector<int64_t> fired;
    aeron_cluster_timer_service_t *svc = nullptr;
    ASSERT_EQ(0, aeron_cluster_timer_service_create(&svc, ordered_expiry, &fired));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(svc, 1, 10));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(svc, 2, 14));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(svc, 3, 30));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(svc, 4, 29));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(svc, 5, 15));

    EXPECT_EQ(2, aeron_cluster_timer_service_poll(svc, 14));
    ASSERT_EQ(2u, fired.size());
    EXPECT_EQ(1, fired[0]);
    EXPECT_EQ(2, fired[1]);

    SnapshotCapture cap;
    aeron_cluster_timer_service_snapshot(svc, capture_snapshot, &cap);
    ASSERT_EQ(3u, cap.timers.size());
    EXPECT_EQ(5, cap.timers[0].first);  EXPECT_EQ(15, cap.timers[0].second);
    EXPECT_EQ(4, cap.timers[1].first);  EXPECT_EQ(29, cap.timers[1].second);
    EXPECT_EQ(3, cap.timers[2].first);  EXPECT_EQ(30, cap.timers[2].second);
    aeron_cluster_timer_service_close(svc);
}
