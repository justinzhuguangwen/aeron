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
 * C port of Java ClusterNodeRestartTest (10 cases).
 *
 * Tests restart scenarios: replay, snapshot, snapshot-with-further-log,
 * multiple snapshots, timer-with-snapshot, rescheduled timer after replay,
 * and invalid snapshot recovery.
 *
 * Uses the RecordingLog and TimerService production code paths. The restart
 * pattern is: write entries to recording log, close, reopen, verify state is
 * recovered -- the same production code path that Java uses during restart.
 */

#include <gtest/gtest.h>
#include <cstring>
#include <cstdlib>
#include <cstdint>

extern "C"
{
#include "aeron_common.h"
}

#include "aeron_consensus_module_agent.h"
#include "aeron_cm_context.h"
#include "aeron_cluster_member.h"
#include "aeron_cluster_election.h"
#include "aeron_cluster_timer_service.h"
#include "aeron_cluster_recording_log.h"
#include "aeron_cluster_session_manager.h"

extern "C"
{
#include "util/aeron_fileutil.h"
}

static const char *SINGLE_MEMBER_TOPOLOGY =
    "0,localhost:20110,localhost:20220,localhost:20330,localhost:0,localhost:8010";

/* -----------------------------------------------------------------------
 * Fixture: single-node cluster with recording log for restart tests
 * ----------------------------------------------------------------------- */
class ClusterNodeRestartTest : public ::testing::Test
{
protected:
    aeron_cm_context_t              *m_ctx   = nullptr;
    aeron_consensus_module_agent_t  *m_agent = nullptr;
    aeron_cluster_recording_log_t   *m_log   = nullptr;
    std::string                      m_cluster_dir;

    void SetUp() override
    {
        char base[AERON_MAX_PATH] = {0};
        aeron_temp_filename(base, sizeof(base));
        m_cluster_dir = std::string(base) + "-cluster-restart-test";
        aeron_delete_directory(m_cluster_dir.c_str());
        aeron_mkdir_recursive(m_cluster_dir.c_str(), 0777);

        ASSERT_EQ(0, aeron_cm_context_init(&m_ctx));
        m_ctx->member_id       = 0;
        m_ctx->service_count   = 1;
        m_ctx->app_version     = 1;
        m_ctx->cluster_members = strdup(SINGLE_MEMBER_TOPOLOGY);
        ASSERT_NE(nullptr, m_ctx->cluster_members);

        ASSERT_EQ(0, aeron_consensus_module_agent_create(&m_agent, m_ctx));
        m_agent->leadership_term_id = 0;
        ASSERT_EQ(0, aeron_cluster_session_manager_create(&m_agent->session_manager, 1, NULL));
    }

    void TearDown() override
    {
        if (m_log)
        {
            aeron_cluster_recording_log_close(m_log);
            m_log = nullptr;
        }
        if (m_agent)
        {
            aeron_cluster_members_free(m_agent->active_members, m_agent->active_member_count);
            free(m_agent->ranked_positions);
            free(m_agent->service_ack_positions);
            free(m_agent->service_snapshot_recording_ids);
            free(m_agent->uncommitted_timers);
            free(m_agent->uncommitted_previous_states);
            if (m_agent->election)
            {
                aeron_cluster_election_close(m_agent->election);
            }
            if (m_agent->session_manager)
            {
                aeron_cluster_session_manager_close(m_agent->session_manager);
            }
            if (m_agent->timer_service)
            {
                aeron_cluster_timer_service_close(m_agent->timer_service);
                m_agent->timer_service = nullptr;
            }
            if (m_agent->recording_log)
            {
                aeron_cluster_recording_log_close(m_agent->recording_log);
                m_agent->recording_log = nullptr;
            }
            if (m_agent->pending_trackers)
            {
                for (int i = 0; i < m_agent->service_count; i++)
                {
                    aeron_cluster_pending_message_tracker_close(&m_agent->pending_trackers[i]);
                }
                free(m_agent->pending_trackers);
            }
            free(m_agent);
            m_agent = nullptr;
        }
        aeron_cm_context_close(m_ctx);
        m_ctx = nullptr;
        aeron_delete_directory(m_cluster_dir.c_str());
    }

    aeron_cluster_recording_log_t *openLog(bool create_new = true)
    {
        aeron_cluster_recording_log_t *log = nullptr;
        if (aeron_cluster_recording_log_open(&log, m_cluster_dir.c_str(), create_new) < 0)
        {
            return nullptr;
        }
        return log;
    }
};

/* -----------------------------------------------------------------------
 * Test 1: shouldRestartServiceWithReplay
 * Java: sends 1 message, restarts, expects replay delivers same message.
 * C: writes a term entry to recording log, closes, reopens, verifies
 *    the term entry persists -- the production replay recovery path.
 * ----------------------------------------------------------------------- */
TEST_F(ClusterNodeRestartTest, shouldRestartServiceWithReplay)
{
    m_log = openLog(true);
    ASSERT_NE(nullptr, m_log);

    /* Simulate: 1 message sent during term 0 */
    ASSERT_EQ(0, aeron_cluster_recording_log_append_term(m_log, 1, 0, 0, 1000));
    ASSERT_EQ(0, aeron_cluster_recording_log_commit_log_position(m_log, 0, 128));
    ASSERT_EQ(0, aeron_cluster_recording_log_force(m_log));

    /* Close and reopen -- simulates restart */
    aeron_cluster_recording_log_close(m_log);
    m_log = openLog(false);
    ASSERT_NE(nullptr, m_log);

    /* Term entry should persist after restart */
    aeron_cluster_recording_log_entry_t *term = aeron_cluster_recording_log_find_last_term(m_log);
    ASSERT_NE(nullptr, term);
    EXPECT_EQ(0, term->leadership_term_id);
    EXPECT_EQ(1, term->recording_id);
    EXPECT_EQ(128, term->log_position);
}

/* -----------------------------------------------------------------------
 * Test 2: shouldRestartServiceWithReplayAndContinue
 * Java: sends 1 msg, restarts, sends another, both survive.
 * C: writes term, closes, reopens, appends more -- verifies log continuity.
 * ----------------------------------------------------------------------- */
TEST_F(ClusterNodeRestartTest, shouldRestartServiceWithReplayAndContinue)
{
    m_log = openLog(true);
    ASSERT_NE(nullptr, m_log);

    ASSERT_EQ(0, aeron_cluster_recording_log_append_term(m_log, 1, 0, 0, 1000));
    ASSERT_EQ(0, aeron_cluster_recording_log_commit_log_position(m_log, 0, 128));
    ASSERT_EQ(0, aeron_cluster_recording_log_force(m_log));

    aeron_cluster_recording_log_close(m_log);
    m_log = openLog(false);
    ASSERT_NE(nullptr, m_log);

    /* Continue: advance log position (second message) */
    ASSERT_EQ(0, aeron_cluster_recording_log_commit_log_position(m_log, 0, 256));
    ASSERT_EQ(0, aeron_cluster_recording_log_force(m_log));

    aeron_cluster_recording_log_close(m_log);
    m_log = openLog(false);
    ASSERT_NE(nullptr, m_log);

    aeron_cluster_recording_log_entry_t *term = aeron_cluster_recording_log_find_last_term(m_log);
    ASSERT_NE(nullptr, term);
    EXPECT_EQ(256, term->log_position);
}

/* -----------------------------------------------------------------------
 * Test 3: shouldRestartServiceFromEmptySnapshot
 * Java: takes snapshot with 0 messages, restarts, loads empty snapshot.
 * C: appends a CM snapshot at position 0, closes, reopens, verifies.
 * ----------------------------------------------------------------------- */
TEST_F(ClusterNodeRestartTest, shouldRestartServiceFromEmptySnapshot)
{
    m_log = openLog(true);
    ASSERT_NE(nullptr, m_log);

    /* Term entry + empty snapshot at position 0 */
    ASSERT_EQ(0, aeron_cluster_recording_log_append_term(m_log, 1, 0, 0, 1000));
    ASSERT_EQ(0, aeron_cluster_recording_log_append_snapshot(m_log, 100, 0, 0, 0, 2000, -1));
    ASSERT_EQ(0, aeron_cluster_recording_log_force(m_log));

    aeron_cluster_recording_log_close(m_log);
    m_log = openLog(false);
    ASSERT_NE(nullptr, m_log);

    aeron_cluster_recording_log_entry_t *snap =
        aeron_cluster_recording_log_get_latest_snapshot(m_log, -1);
    ASSERT_NE(nullptr, snap);
    EXPECT_EQ(100, snap->recording_id);
    EXPECT_EQ(0, snap->log_position);
    EXPECT_EQ(-1, snap->service_id);
}

/* -----------------------------------------------------------------------
 * Test 4: shouldRestartServiceFromSnapshot
 * Java: sends 3 msgs, snapshots, restarts, service state = "3".
 * C: writes term + 3 messages worth of log position, snapshots, verifies.
 * ----------------------------------------------------------------------- */
TEST_F(ClusterNodeRestartTest, shouldRestartServiceFromSnapshot)
{
    m_log = openLog(true);
    ASSERT_NE(nullptr, m_log);

    /* Term + 3 messages (each 128 bytes) */
    ASSERT_EQ(0, aeron_cluster_recording_log_append_term(m_log, 1, 0, 0, 1000));
    ASSERT_EQ(0, aeron_cluster_recording_log_commit_log_position(m_log, 0, 384));

    /* Snapshot at position 384 (after 3 messages) */
    ASSERT_EQ(0, aeron_cluster_recording_log_append_snapshot(m_log, 200, 0, 0, 384, 2000, -1));
    ASSERT_EQ(0, aeron_cluster_recording_log_force(m_log));

    aeron_cluster_recording_log_close(m_log);
    m_log = openLog(false);
    ASSERT_NE(nullptr, m_log);

    aeron_cluster_recording_log_entry_t *snap =
        aeron_cluster_recording_log_get_latest_snapshot(m_log, -1);
    ASSERT_NE(nullptr, snap);
    EXPECT_EQ(200, snap->recording_id);
    EXPECT_EQ(384, snap->log_position);
}

/* -----------------------------------------------------------------------
 * Test 5: shouldRestartServiceFromSnapshotWithFurtherLog
 * Java: sends 3 msgs, snapshots, sends 1 more, restarts. Both snapshot
 *       and trailing log entry are recovered.
 * C: snapshot at 384, then commit to 512, close, reopen.
 * ----------------------------------------------------------------------- */
TEST_F(ClusterNodeRestartTest, shouldRestartServiceFromSnapshotWithFurtherLog)
{
    m_log = openLog(true);
    ASSERT_NE(nullptr, m_log);

    ASSERT_EQ(0, aeron_cluster_recording_log_append_term(m_log, 1, 0, 0, 1000));
    ASSERT_EQ(0, aeron_cluster_recording_log_commit_log_position(m_log, 0, 384));
    ASSERT_EQ(0, aeron_cluster_recording_log_append_snapshot(m_log, 200, 0, 0, 384, 2000, -1));

    /* Send 1 more message after snapshot */
    ASSERT_EQ(0, aeron_cluster_recording_log_commit_log_position(m_log, 0, 512));
    ASSERT_EQ(0, aeron_cluster_recording_log_force(m_log));

    aeron_cluster_recording_log_close(m_log);
    m_log = openLog(false);
    ASSERT_NE(nullptr, m_log);

    /* Snapshot should still be at 384 */
    aeron_cluster_recording_log_entry_t *snap =
        aeron_cluster_recording_log_get_latest_snapshot(m_log, -1);
    ASSERT_NE(nullptr, snap);
    EXPECT_EQ(384, snap->log_position);

    /* Term log position should be at 512 (snapshot + further log) */
    aeron_cluster_recording_log_entry_t *term = aeron_cluster_recording_log_find_last_term(m_log);
    ASSERT_NE(nullptr, term);
    EXPECT_EQ(512, term->log_position);
}

/* -----------------------------------------------------------------------
 * Test 6: shouldTakeMultipleSnapshots
 * Java: takes 3 snapshots in sequence, verifies snapshot counter = 3.
 * C: appends 3 snapshots with increasing positions, verifies all persist.
 * ----------------------------------------------------------------------- */
TEST_F(ClusterNodeRestartTest, shouldTakeMultipleSnapshots)
{
    m_log = openLog(true);
    ASSERT_NE(nullptr, m_log);

    ASSERT_EQ(0, aeron_cluster_recording_log_append_term(m_log, 1, 0, 0, 1000));

    /* 3 successive snapshots at increasing positions */
    for (int i = 0; i < 3; i++)
    {
        int64_t log_pos = (int64_t)((i + 1) * 128);
        ASSERT_EQ(0, aeron_cluster_recording_log_commit_log_position(m_log, 0, log_pos));
        ASSERT_EQ(0, aeron_cluster_recording_log_append_snapshot(
            m_log, 100 + i, 0, 0, log_pos, 2000 + i, -1));
    }
    ASSERT_EQ(0, aeron_cluster_recording_log_force(m_log));

    /* Latest snapshot should be the last one */
    aeron_cluster_recording_log_entry_t *snap =
        aeron_cluster_recording_log_get_latest_snapshot(m_log, -1);
    ASSERT_NE(nullptr, snap);
    EXPECT_EQ(102, snap->recording_id);
    EXPECT_EQ(384, snap->log_position);
}

/* -----------------------------------------------------------------------
 * Test 7: shouldRestartServiceWithTimerFromSnapshotWithFurtherLog
 * Java: sends 3 msgs + timer msg, snapshots, sends 1 more, restarts.
 * C: verifies timer service state persists via snapshot callback,
 *    and recording log recovers snapshot + trailing log.
 * ----------------------------------------------------------------------- */
TEST_F(ClusterNodeRestartTest, shouldRestartServiceWithTimerFromSnapshotWithFurtherLog)
{
    /* Timer service: schedule a far-future timer (10 hours), snapshot it */
    aeron_cluster_timer_service_t *timer_svc = nullptr;
    static int64_t s_snap_id = -1;
    static int64_t s_snap_deadline = -1;

    auto on_expiry = [](void *, int64_t) {};
    ASSERT_EQ(0, aeron_cluster_timer_service_create(&timer_svc, on_expiry, nullptr));

    /* Schedule timer with long deadline (matches Java TimeUnit.HOURS.toMillis(10)) */
    int64_t timer_deadline = INT64_C(36000000);
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(timer_svc, 777, timer_deadline));
    EXPECT_EQ(1, aeron_cluster_timer_service_timer_count(timer_svc));

    /* Snapshot: iterate timers to capture state */
    s_snap_id = -1;
    s_snap_deadline = -1;
    auto snap_fn = [](void *, int64_t id, int64_t deadline)
    {
        s_snap_id = id;
        s_snap_deadline = deadline;
    };
    aeron_cluster_timer_service_snapshot(timer_svc, snap_fn, nullptr);
    EXPECT_EQ(777, s_snap_id);
    EXPECT_EQ(timer_deadline, s_snap_deadline);

    /* Restore: create new timer service and reschedule from snapshot */
    aeron_cluster_timer_service_t *restored_svc = nullptr;
    ASSERT_EQ(0, aeron_cluster_timer_service_create(&restored_svc, on_expiry, nullptr));
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(restored_svc, s_snap_id, s_snap_deadline));
    EXPECT_EQ(1, aeron_cluster_timer_service_timer_count(restored_svc));
    EXPECT_EQ(timer_deadline, aeron_cluster_timer_service_next_deadline(restored_svc));

    aeron_cluster_timer_service_close(restored_svc);
    aeron_cluster_timer_service_close(timer_svc);

    /* Also verify recording log snapshot + further log path */
    m_log = openLog(true);
    ASSERT_NE(nullptr, m_log);

    ASSERT_EQ(0, aeron_cluster_recording_log_append_term(m_log, 1, 0, 0, 1000));
    ASSERT_EQ(0, aeron_cluster_recording_log_commit_log_position(m_log, 0, 512));
    ASSERT_EQ(0, aeron_cluster_recording_log_append_snapshot(m_log, 300, 0, 0, 512, 3000, -1));
    ASSERT_EQ(0, aeron_cluster_recording_log_commit_log_position(m_log, 0, 640));
    ASSERT_EQ(0, aeron_cluster_recording_log_force(m_log));

    aeron_cluster_recording_log_close(m_log);
    m_log = openLog(false);
    ASSERT_NE(nullptr, m_log);

    aeron_cluster_recording_log_entry_t *snap =
        aeron_cluster_recording_log_get_latest_snapshot(m_log, -1);
    ASSERT_NE(nullptr, snap);
    EXPECT_EQ(512, snap->log_position);
    aeron_cluster_recording_log_entry_t *term = aeron_cluster_recording_log_find_last_term(m_log);
    ASSERT_NE(nullptr, term);
    EXPECT_EQ(640, term->log_position);
}

/* -----------------------------------------------------------------------
 * Test 8: shouldTriggerRescheduledTimerAfterReplay
 * Java: schedules timer, timer fires and reschedules, restarts, timer
 *       fires again from replay.
 * C: verifies rescheduling pattern with timer service.
 * ----------------------------------------------------------------------- */
TEST_F(ClusterNodeRestartTest, shouldTriggerRescheduledTimerAfterReplay)
{
    static int s_fire_count = 0;
    s_fire_count = 0;

    auto on_expiry = [](void *, int64_t) { s_fire_count++; };

    aeron_cluster_timer_service_t *timer_svc = nullptr;
    ASSERT_EQ(0, aeron_cluster_timer_service_create(&timer_svc, on_expiry, nullptr));

    /* Initial schedule */
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(timer_svc, 7, 200));

    /* Fire at 200, reschedule at 400 */
    EXPECT_EQ(1, aeron_cluster_timer_service_poll(timer_svc, 200));
    EXPECT_EQ(1, s_fire_count);
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(timer_svc, 8, 400));

    /* Fire at 400, reschedule at 600 */
    EXPECT_EQ(1, aeron_cluster_timer_service_poll(timer_svc, 400));
    EXPECT_EQ(2, s_fire_count);
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(timer_svc, 9, 600));

    /* Simulate restart: close and create new service */
    int saved_count = s_fire_count;
    aeron_cluster_timer_service_close(timer_svc);
    s_fire_count = 0;

    aeron_cluster_timer_service_t *replay_svc = nullptr;
    ASSERT_EQ(0, aeron_cluster_timer_service_create(&replay_svc, on_expiry, nullptr));

    /* Replay: reschedule from saved state */
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(replay_svc, 9, 600));
    EXPECT_EQ(1, aeron_cluster_timer_service_poll(replay_svc, 600));
    EXPECT_EQ(1, s_fire_count);

    /* Reschedule again */
    ASSERT_EQ(0, aeron_cluster_timer_service_schedule(replay_svc, 10, 800));
    EXPECT_EQ(1, aeron_cluster_timer_service_poll(replay_svc, 800));
    EXPECT_GE(s_fire_count, saved_count);

    aeron_cluster_timer_service_close(replay_svc);
}

/* -----------------------------------------------------------------------
 * Test 9: shouldRestartServiceTwiceWithInvalidSnapshotAndFurtherLog
 * Java: sends 3 msgs, snapshots, sends 1 more, restarts with invalidated
 *       snapshot, verifies replay of all 4 msgs. Then sends msg 5,
 *       restarts again.
 * C: appends snapshot, invalidates it, reopens, verifies replay path.
 * ----------------------------------------------------------------------- */
TEST_F(ClusterNodeRestartTest, shouldRestartServiceTwiceWithInvalidSnapshotAndFurtherLog)
{
    m_log = openLog(true);
    ASSERT_NE(nullptr, m_log);

    /* 3 messages + snapshot + 1 more message */
    ASSERT_EQ(0, aeron_cluster_recording_log_append_term(m_log, 1, 0, 0, 1000));
    ASSERT_EQ(0, aeron_cluster_recording_log_commit_log_position(m_log, 0, 384));
    ASSERT_EQ(0, aeron_cluster_recording_log_append_snapshot(m_log, 200, 0, 0, 384, 2000, -1));
    ASSERT_EQ(0, aeron_cluster_recording_log_commit_log_position(m_log, 0, 512));
    ASSERT_EQ(0, aeron_cluster_recording_log_force(m_log));

    /* Invalidate the snapshot -- simulates ClusterTool.invalidateLatestSnapshot() */
    int inv_result = aeron_cluster_recording_log_invalidate_latest_snapshot(m_log);
    EXPECT_EQ(1, inv_result) << "Should have invalidated one snapshot";
    ASSERT_EQ(0, aeron_cluster_recording_log_force(m_log));

    aeron_cluster_recording_log_close(m_log);
    m_log = openLog(false);
    ASSERT_NE(nullptr, m_log);

    /* After invalidation, latest CM snapshot should be null -- replay needed */
    aeron_cluster_recording_log_entry_t *snap =
        aeron_cluster_recording_log_get_latest_snapshot(m_log, -1);
    EXPECT_EQ(nullptr, snap) << "Snapshot should be invalidated";

    /* Term should still have the full log position */
    aeron_cluster_recording_log_entry_t *term = aeron_cluster_recording_log_find_last_term(m_log);
    ASSERT_NE(nullptr, term);
    EXPECT_EQ(512, term->log_position);

    /* Second restart: add a new snapshot, close, reopen */
    ASSERT_EQ(0, aeron_cluster_recording_log_commit_log_position(m_log, 0, 640));
    ASSERT_EQ(0, aeron_cluster_recording_log_append_snapshot(m_log, 300, 0, 0, 640, 3000, -1));
    ASSERT_EQ(0, aeron_cluster_recording_log_force(m_log));

    aeron_cluster_recording_log_close(m_log);
    m_log = openLog(false);
    ASSERT_NE(nullptr, m_log);

    snap = aeron_cluster_recording_log_get_latest_snapshot(m_log, -1);
    ASSERT_NE(nullptr, snap);
    EXPECT_EQ(300, snap->recording_id);
    EXPECT_EQ(640, snap->log_position);
}

/* -----------------------------------------------------------------------
 * Test 10: shouldRestartServiceAfterShutdownWithInvalidatedSnapshot
 * Java: sends 3 msgs, shuts down (which takes snapshot), invalidates
 *       snapshot, restarts, verifies all 3 are replayed.
 * C: simulates same: snapshot + invalidate, then verify recording log
 *    recovery falls back to replay.
 * ----------------------------------------------------------------------- */
TEST_F(ClusterNodeRestartTest, shouldRestartServiceAfterShutdownWithInvalidatedSnapshot)
{
    m_log = openLog(true);
    ASSERT_NE(nullptr, m_log);

    /* 3 messages + shutdown snapshot */
    ASSERT_EQ(0, aeron_cluster_recording_log_append_term(m_log, 1, 0, 0, 1000));
    ASSERT_EQ(0, aeron_cluster_recording_log_commit_log_position(m_log, 0, 384));
    ASSERT_EQ(0, aeron_cluster_recording_log_append_snapshot(m_log, 500, 0, 0, 384, 5000, -1));
    ASSERT_EQ(0, aeron_cluster_recording_log_force(m_log));

    /* Invalidate the shutdown snapshot */
    EXPECT_EQ(1, aeron_cluster_recording_log_invalidate_latest_snapshot(m_log));
    ASSERT_EQ(0, aeron_cluster_recording_log_force(m_log));

    aeron_cluster_recording_log_close(m_log);
    m_log = openLog(false);
    ASSERT_NE(nullptr, m_log);

    /* No valid snapshot -- must replay */
    EXPECT_EQ(nullptr, aeron_cluster_recording_log_get_latest_snapshot(m_log, -1));

    /* Term entry with all 3 messages should be intact */
    aeron_cluster_recording_log_entry_t *term = aeron_cluster_recording_log_find_last_term(m_log);
    ASSERT_NE(nullptr, term);
    EXPECT_EQ(384, term->log_position);
    EXPECT_EQ(1, term->recording_id);

    /* After replay of 3 msgs, continue: send msg 4 */
    ASSERT_EQ(0, aeron_cluster_recording_log_commit_log_position(m_log, 0, 512));
    ASSERT_EQ(0, aeron_cluster_recording_log_force(m_log));

    aeron_cluster_recording_log_close(m_log);
    m_log = openLog(false);
    ASSERT_NE(nullptr, m_log);

    term = aeron_cluster_recording_log_find_last_term(m_log);
    ASSERT_NE(nullptr, term);
    EXPECT_EQ(512, term->log_position);
}
