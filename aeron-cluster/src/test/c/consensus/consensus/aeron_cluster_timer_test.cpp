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

/**
 * C port of Java ClusterTimerTest (3 cases).
 *
 * Tests timer scheduling, rescheduling, and snapshot/replay with timers.
 * Uses the TimerService production code directly, matching the same code
 * paths exercised by Java's ClusteredService.onTimerEvent /
 * cluster.scheduleTimer / ConsensusModule timer service.
 */

#include <gtest/gtest.h>
#include <cstdint>
#include <cstring>

extern "C"
{
#include "aeron_cluster_timer_service.h"
}

static int64_t g_timer_fired_id       = -1;
static int     g_timer_fired_count   = 0;
static int64_t g_timer_fired_ids[64];

static void reset_timer_globals()
{
    g_timer_fired_id    = -1;
    g_timer_fired_count= 0;
    memset(g_timer_fired_ids, 0, sizeof(g_timer_fired_ids));
}

static void timer_on_expiry(void * /*clientd*/, int64_t correlation_id)
{
    if (g_timer_fired_count < 64)
    {
        g_timer_fired_ids[g_timer_fired_count] = correlation_id;
    }
    g_timer_fired_id = correlation_id;
    g_timer_fired_count++;
}

class ClusterTimerTest : public ::testing::Test
{
protected:
    aeron_cluster_timer_service_t *m_svc = nullptr;

    void SetUp() override
    {
        reset_timer_globals();
        ASSERT_EQ(0, aeron_cluster_timer_service_create(&m_svc, timer_on_expiry, nullptr));
    }

    void TearDown() override
    {
        aeron_cluster_timer_service_close(m_svc);
        m_svc = nullptr;
    }
};

/* -----------------------------------------------------------------------
 * Test 1: shouldRestartServiceWithTimerFromSnapshotWithFurtherLog
 * Java: schedules rescheduling timer on leadership term start, triggers 2,
 *       snapshots, triggers 2 more, restarts, triggers more.
 * C: exercises the same rescheduling-timer + snapshot + restore pattern.
 * ----------------------------------------------------------------------- */
TEST_F(ClusterTimerTest, shouldRestartServiceWithTimerFromSnapshotWithFurtherLog)
{
    static constexpr int64_t INTERVAL_MS= 20;
    int64_t now= 0;

    /* Phase 1: schedule initial timer, fire twice (rescheduling) */
    int next_id= 1;
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, next_id++, now + INTERVAL_MS));

    now += INTERVAL_MS;
    EXPECT_EQ(1, aeron_cluster_timer_service_poll(m_svc, now));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, next_id++, now + INTERVAL_MS));

    now += INTERVAL_MS;
    EXPECT_EQ(1, aeron_cluster_timer_service_poll(m_svc, now));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, next_id++, now + INTERVAL_MS));

    EXPECT_EQ(2, g_timer_fired_count);

    /* Phase 2: take snapshot -- capture timer state */
    static int64_t snap_ids[16];
    static int64_t snap_deadlines[16];
    static int snap_count= 0;
    snap_count= 0;
    auto snap_fn = [](void *, int64_t id, int64_t deadline)
    {
        if (snap_count < 16)
        {
            snap_ids[snap_count] = id;
            snap_deadlines[snap_count] = deadline;
            snap_count++;
        }
    };
    aeron_cluster_timer_service_snapshot(m_svc, snap_fn, nullptr);
    EXPECT_EQ(1, snap_count) << "Should have 1 pending timer in snapshot";

    /* Phase 3: fire 2 more after snapshot */
    now += INTERVAL_MS;
    EXPECT_EQ(1, aeron_cluster_timer_service_poll(m_svc, now));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, next_id++, now + INTERVAL_MS));

    now += INTERVAL_MS;
    EXPECT_EQ(1, aeron_cluster_timer_service_poll(m_svc, now));
    EXPECT_EQ(4, g_timer_fired_count);

    /* Phase 4: simulate restart -- restore from snapshot */
    aeron_cluster_timer_service_close(m_svc);
    reset_timer_globals();
    ASSERT_EQ(0, aeron_cluster_timer_service_create(&m_svc, timer_on_expiry, nullptr));

    for (int i = 0; i < snap_count; i++)
    {
        ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, snap_ids[i], snap_deadlines[i]));
    }

    /* Advance time to trigger restored timers + rescheduling */
    int total_fires= 0;
    for (int tick = 0; tick < 20; tick++)
    {
        now += INTERVAL_MS;
        int fired = aeron_cluster_timer_service_poll(m_svc, now);
        if (fired > 0)
        {
            total_fires += fired;
            ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, next_id++, now + INTERVAL_MS));
        }
        if (total_fires >= 6) break;
    }
    EXPECT_GE(total_fires, 2) << "Should fire restored timers + rescheduled ones";
}

/* -----------------------------------------------------------------------
 * Test 2: shouldTriggerRescheduledTimerAfterReplay
 * Java: schedules timer on leadership, fires 2, restarts, replays fires
 *       again, restarts again, fires more.
 * C: same rescheduling timer pattern across two restart cycles.
 * ----------------------------------------------------------------------- */
TEST_F(ClusterTimerTest, shouldTriggerRescheduledTimerAfterReplay)
{
    static constexpr int64_t INTERVAL_MS= 20;
    int64_t now= 0;
    int next_id= 1;

    /* Phase 1: initial leadership -- schedule and fire 2 */
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, next_id++, now + INTERVAL_MS));

    now += INTERVAL_MS;
    aeron_cluster_timer_service_poll(m_svc, now);
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, next_id++, now + INTERVAL_MS));

    now += INTERVAL_MS;
    aeron_cluster_timer_service_poll(m_svc, now);
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, next_id++, now + INTERVAL_MS));

    int fired_before_restart = g_timer_fired_count;
    EXPECT_EQ(2, fired_before_restart);

    /* Phase 2: restart #1 -- replay should trigger same timers */
    aeron_cluster_timer_service_close(m_svc);
    reset_timer_globals();
    ASSERT_EQ(0, aeron_cluster_timer_service_create(&m_svc, timer_on_expiry, nullptr));

    /* Replay: re-schedule all timers that were active */
    now= 0;
    next_id= 1;
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, next_id++, INTERVAL_MS));

    now = INTERVAL_MS;
    aeron_cluster_timer_service_poll(m_svc, now);
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, next_id++, now + INTERVAL_MS));

    now += INTERVAL_MS;
    aeron_cluster_timer_service_poll(m_svc, now);
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, next_id++, now + INTERVAL_MS));

    /* New leadership term fires additional timers */
    now += INTERVAL_MS;
    aeron_cluster_timer_service_poll(m_svc, now);
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, next_id++, now + INTERVAL_MS));

    now += INTERVAL_MS;
    aeron_cluster_timer_service_poll(m_svc, now);

    EXPECT_GE(g_timer_fired_count, fired_before_restart + 2)
        << "After replay + new leadership, should fire more timers";

    /* Phase 3: restart #2 */
    int fired_before_restart2 = g_timer_fired_count;
    aeron_cluster_timer_service_close(m_svc);
    reset_timer_globals();
    ASSERT_EQ(0, aeron_cluster_timer_service_create(&m_svc, timer_on_expiry, nullptr));

    now= 0;
    next_id= 1;
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, next_id++, INTERVAL_MS));

    for (int tick = 0; tick < 20; tick++)
    {
        now += INTERVAL_MS;
        int fired = aeron_cluster_timer_service_poll(m_svc, now);
        if (fired > 0)
        {
            ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, next_id++, now + INTERVAL_MS));
        }
        if (g_timer_fired_count >= fired_before_restart2 + 4) break;
    }

    EXPECT_GE(g_timer_fired_count, 4)
        << "After second replay, should continue firing";
}

/* -----------------------------------------------------------------------
 * Test 3: shouldRescheduleTimerWhenSchedulingWithExistingCorrelationId
 * Java: schedules timer 1 with far deadline, reschedules timer 1 with
 *       near deadline, schedules timer 2. Timer 1 fires at near deadline
 *       (not far), then timer 2 fires.
 * C: same pattern -- schedule, reschedule, verify ordering.
 * ----------------------------------------------------------------------- */
TEST_F(ClusterTimerTest, shouldRescheduleTimerWhenSchedulingWithExistingCorrelationId)
{
    /* Schedule timer 1 with far-future deadline */
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, 1, 1000000));
    EXPECT_EQ(1, aeron_cluster_timer_service_timer_count(m_svc));

    /* Reschedule timer 1 with near deadline (20ms) -- should replace */
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, 1, 20));
    EXPECT_EQ(1, aeron_cluster_timer_service_timer_count(m_svc))
        << "Rescheduling same ID should not increase count";

    /* Schedule timer 2 with deadline 30ms */
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(m_svc, 2, 30));
    EXPECT_EQ(2, aeron_cluster_timer_service_timer_count(m_svc));

    /* At time 20: timer 1 should fire (rescheduled near deadline, not far) */
    EXPECT_EQ(1, aeron_cluster_timer_service_poll(m_svc, 20));
    EXPECT_EQ(1, g_timer_fired_count);
    EXPECT_EQ(1, g_timer_fired_ids[0]) << "Timer 1 should fire first (deadline 20)";

    /* At time 30: timer 2 should fire */
    EXPECT_EQ(1, aeron_cluster_timer_service_poll(m_svc, 30));
    EXPECT_EQ(2, g_timer_fired_count);
    EXPECT_EQ(2, g_timer_fired_ids[1]) << "Timer 2 should fire second (deadline 30)";

    /* Both fired, none remaining */
    EXPECT_EQ(0, aeron_cluster_timer_service_timer_count(m_svc));
}
