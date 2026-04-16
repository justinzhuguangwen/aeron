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
#include <cstring>
#include <cstdlib>
#include <cstdint>
#include <string>

extern "C"
{
#include "aeron_cluster_session_manager.h"
#include "aeron_common.h"
#include "util/aeron_fileutil.h"
}

static std::string make_test_dir(const char *prefix)
{
    char base[AERON_MAX_PATH] = {0};
    aeron_temp_filename(base, sizeof(base));
    return std::string(base) + "-" + prefix;
}

/* =======================================================================
 * SessionManagerTest
 * ======================================================================= */

class SessionManagerTest : public ::testing::Test
{
protected:
    aeron_cluster_session_manager_t *m_mgr = nullptr;

    void SetUp() override
    {
        ASSERT_EQ(0, aeron_cluster_session_manager_create(&m_mgr, 1, nullptr));
    }

    void TearDown() override
    {
        aeron_cluster_session_manager_close(m_mgr);
    }
};

TEST_F(SessionManagerTest, initialState)
{
    EXPECT_EQ(0, aeron_cluster_session_manager_session_count(m_mgr));
}

TEST_F(SessionManagerTest, onReplaySessionOpenAddsSessions)
{
    aeron_cluster_session_manager_on_replay_session_open(
        m_mgr, 100, 42, 7, 12345678, 101, "aeron:udp?endpoint=localhost:9999");
    EXPECT_EQ(1, aeron_cluster_session_manager_session_count(m_mgr));
    auto *s = aeron_cluster_session_manager_find(m_mgr, 7);
    ASSERT_NE(nullptr, s);
    EXPECT_EQ(7LL, s->id);
    EXPECT_EQ(100LL, s->opened_log_position);
    EXPECT_EQ(AERON_CLUSTER_SESSION_STATE_OPEN, s->state);
}

TEST_F(SessionManagerTest, onReplaySessionOpenAdvancesNextId)
{
    /* Session 10 > initial_session_id=1 → nextSessionId should become 11 */
    aeron_cluster_session_manager_on_replay_session_open(
        m_mgr, 0, 0, 10, 0, 1, "ch");
    EXPECT_EQ(1, aeron_cluster_session_manager_session_count(m_mgr));
    /* Next new session should get id ≥ 11 */
}

TEST_F(SessionManagerTest, onReplaySessionCloseRemovesSession)
{
    aeron_cluster_session_manager_on_replay_session_open(
        m_mgr, 0, 0, 5, 0, 1, "ch");
    EXPECT_EQ(1, aeron_cluster_session_manager_session_count(m_mgr));

    aeron_cluster_session_manager_on_replay_session_close(m_mgr, 5, 1);
    EXPECT_EQ(0, aeron_cluster_session_manager_session_count(m_mgr));
    EXPECT_EQ(nullptr, aeron_cluster_session_manager_find(m_mgr, 5));
}

TEST_F(SessionManagerTest, loadClusterSession)
{
    aeron_cluster_session_manager_on_load_cluster_session(
        m_mgr, 20, 100, 500, 9999999, 0, 1, "aeron:udp?endpoint=localhost:1234");
    EXPECT_EQ(1, aeron_cluster_session_manager_session_count(m_mgr));
    auto *s = aeron_cluster_session_manager_find(m_mgr, 20);
    ASSERT_NE(nullptr, s);
    EXPECT_EQ(500LL, s->opened_log_position);
    EXPECT_EQ(AERON_CLUSTER_SESSION_STATE_OPEN, s->state);
}

TEST_F(SessionManagerTest, loadNextSessionId)
{
    aeron_cluster_session_manager_load_next_session_id(m_mgr, 100);
    /* next session allocated should get id 100 */
    auto *s = aeron_cluster_session_manager_new_session(m_mgr, 0, 1, "ch", nullptr, 0);
    ASSERT_NE(nullptr, s);
    EXPECT_EQ(100LL, s->id);
}

TEST_F(SessionManagerTest, snapshotSessionsOnlyOpenAndClosing)
{
    aeron_cluster_session_manager_on_replay_session_open(m_mgr, 0, 0, 1, 0, 1, "ch");
    aeron_cluster_session_manager_on_replay_session_open(m_mgr, 0, 0, 2, 0, 1, "ch");

    /* Close session 2 */
    auto *s2 = aeron_cluster_session_manager_find(m_mgr, 2);
    aeron_cluster_cluster_session_closing(s2, 1);

    int count = 0;
    aeron_cluster_session_manager_snapshot_sessions(
        m_mgr,
        [](void *clientd, const aeron_cluster_cluster_session_t *s) {
            (*static_cast<int *>(clientd))++;
        },
        &count);
    EXPECT_EQ(2, count); /* both OPEN and CLOSING included */
}

TEST_F(SessionManagerTest, prepareForNewTermStartupClosesOpenSessions)
{
    aeron_cluster_session_manager_on_replay_session_open(m_mgr, 0, 0, 1, 0, 1, "ch");
    auto *s = aeron_cluster_session_manager_find(m_mgr, 1);
    EXPECT_EQ(AERON_CLUSTER_SESSION_STATE_OPEN, s->state);

    aeron_cluster_session_manager_prepare_for_new_term(m_mgr, true, 0);
    EXPECT_EQ(AERON_CLUSTER_SESSION_STATE_CLOSING, s->state);
}

TEST_F(SessionManagerTest, prepareForNewTermNonStartupSetsNewLeaderPending)
{
    aeron_cluster_session_manager_on_replay_session_open(m_mgr, 0, 0, 1, 0, 1, "ch");
    auto *s = aeron_cluster_session_manager_find(m_mgr, 1);

    aeron_cluster_session_manager_prepare_for_new_term(m_mgr, false, 999999LL);
    EXPECT_TRUE(s->has_new_leader_event_pending);
    EXPECT_EQ(999999LL, s->time_of_last_activity_ns);
}

TEST_F(SessionManagerTest, clearSessionsAfterRemovesOpenedAfterPosition)
{
    aeron_cluster_session_manager_on_replay_session_open(m_mgr, 100, 0, 1, 0, 1, "ch");
    aeron_cluster_session_manager_on_replay_session_open(m_mgr, 200, 0, 2, 0, 1, "ch");

    /* Clear sessions opened after position 150 → removes session 2 */
    aeron_cluster_session_manager_clear_sessions_after(m_mgr, 150, 1);
    EXPECT_EQ(1, aeron_cluster_session_manager_session_count(m_mgr));
    EXPECT_NE(nullptr, aeron_cluster_session_manager_find(m_mgr, 1));
    EXPECT_EQ(nullptr, aeron_cluster_session_manager_find(m_mgr, 2));
}

TEST_F(SessionManagerTest, updateActivitySetsTimestamp)
{
    aeron_cluster_session_manager_on_replay_session_open(m_mgr, 0, 0, 1, 0, 1, "ch");
    auto *s = aeron_cluster_session_manager_find(m_mgr, 1);
    s->time_of_last_activity_ns = 0;

    aeron_cluster_session_manager_update_activity(m_mgr, 123456789LL);
    EXPECT_EQ(123456789LL, s->time_of_last_activity_ns);
}

static void count_timeout_cb(void *clientd, aeron_cluster_cluster_session_t *)
{
    (*reinterpret_cast<int *>(clientd))++;
}

TEST_F(SessionManagerTest, checkTimeoutsRemovesExpiredSession)
{
    aeron_cluster_session_manager_on_replay_session_open(m_mgr, 0, 0, 50, 0, 1, "ch");
    auto *s = aeron_cluster_session_manager_find(m_mgr, 50);
    ASSERT_NE(nullptr, s);
    s->time_of_last_activity_ns = 0;  /* very old */

    int timeout_count = 0;
    int removed = aeron_cluster_session_manager_check_timeouts(
        m_mgr, 1000000LL, 999LL, count_timeout_cb, &timeout_count);
    EXPECT_EQ(1, removed);
    EXPECT_EQ(1, timeout_count);
    EXPECT_EQ(0, aeron_cluster_session_manager_session_count(m_mgr));
}

TEST_F(SessionManagerTest, checkTimeoutsKeepsFreshSession)
{
    aeron_cluster_session_manager_on_replay_session_open(m_mgr, 0, 0, 51, 0, 1, "ch");
    auto *s = aeron_cluster_session_manager_find(m_mgr, 51);
    ASSERT_NE(nullptr, s);
    s->time_of_last_activity_ns = 999001LL;  /* fresh: 1000000-999001=999, not > 999999 */

    int timeout_count = 0;
    int removed = aeron_cluster_session_manager_check_timeouts(
        m_mgr, 1000000LL, 999999LL, count_timeout_cb, &timeout_count);
    EXPECT_EQ(0, removed);
    EXPECT_EQ(0, timeout_count);
    EXPECT_EQ(1, aeron_cluster_session_manager_session_count(m_mgr));
}

TEST_F(SessionManagerTest, checkTimeoutsOnlyRemovesExpiredOfMultiple)
{
    aeron_cluster_session_manager_on_replay_session_open(m_mgr, 0, 0, 60, 0, 1, "ch");
    aeron_cluster_session_manager_on_replay_session_open(m_mgr, 0, 0, 61, 0, 1, "ch");
    aeron_cluster_session_manager_on_replay_session_open(m_mgr, 0, 0, 62, 0, 1, "ch");

    auto *s60 = aeron_cluster_session_manager_find(m_mgr, 60);
    auto *s61 = aeron_cluster_session_manager_find(m_mgr, 61);
    auto *s62 = aeron_cluster_session_manager_find(m_mgr, 62);
    s60->time_of_last_activity_ns = 0;        /* old → times out: 1e6 - 0 = 1e6 > 499999 */
    s61->time_of_last_activity_ns = 500000LL; /* old → times out: 1e6 - 500000 = 500000 > 499999 */
    s62->time_of_last_activity_ns = 999001LL; /* fresh: 1e6 - 999001 = 999 is NOT > 499999 */

    int timeout_count = 0;
    int removed = aeron_cluster_session_manager_check_timeouts(
        m_mgr, 1000000LL, 499999LL, count_timeout_cb, &timeout_count);
    EXPECT_EQ(2, removed);
    EXPECT_EQ(2, timeout_count);
    EXPECT_EQ(1, aeron_cluster_session_manager_session_count(m_mgr));
}

/* Helper: directly enqueue a session into the uncommitted ring (bypasses log_publisher).
 * Used to test restore/sweep without needing a real publication.
 * NOTE: aeron_cluster_session_manager_remove() calls close_and_free, so we must
 * do a raw swap-remove from the sessions array to avoid freeing the pointer before
 * storing it in the ring. */
static aeron_cluster_cluster_session_t *make_uncommitted_session(
    aeron_cluster_session_manager_t *mgr,
    int64_t session_id, int64_t closed_log_position)
{
    aeron_cluster_cluster_session_t *s =
        aeron_cluster_session_manager_new_session(mgr, 0, 1, "ch", nullptr, 0);
    if (!s) { return nullptr; }
    s->id                 = session_id;
    s->state              = AERON_CLUSTER_SESSION_STATE_CLOSING;
    s->close_reason       = 1;
    s->closed_log_position = closed_log_position;

    /* Raw swap-remove from active list WITHOUT freeing — remove() would free it. */
    for (int i = 0; i < mgr->session_count; i++)
    {
        if (mgr->sessions[i] == s)
        {
            mgr->sessions[i] = mgr->sessions[--mgr->session_count];
            break;
        }
    }

    /* Push directly into uncommitted ring */
    int next = (mgr->uncommitted_closed_tail + 1) % mgr->uncommitted_closed_capacity;
    mgr->uncommitted_closed[mgr->uncommitted_closed_tail] = s;
    mgr->uncommitted_closed_tail = next;
    return s;
}

/* Session 23: restore_uncommitted_sessions */
TEST_F(SessionManagerTest, restoreUncommittedSessionsRestoresUncommittedClosed)
{
    /* Session 200 was closed at position 500 (not committed at commit_pos=300) */
    make_uncommitted_session(m_mgr, 200, 500LL);
    ASSERT_EQ(0, aeron_cluster_session_manager_session_count(m_mgr));

    aeron_cluster_session_manager_restore_uncommitted_sessions(m_mgr, 300LL);

    /* Session should be back in active list with state=OPEN */
    EXPECT_EQ(1, aeron_cluster_session_manager_session_count(m_mgr));
    auto *s = aeron_cluster_session_manager_find(m_mgr, 200);
    ASSERT_NE(nullptr, s);
    EXPECT_EQ(AERON_CLUSTER_SESSION_STATE_OPEN, s->state);
    EXPECT_EQ(-1LL, s->closed_log_position);
}

TEST_F(SessionManagerTest, restoreUncommittedSessionsFreesCommittedClosed)
{
    /* Session 201 was closed at position 100 (committed at commit_pos=300) */
    make_uncommitted_session(m_mgr, 201, 100LL);
    ASSERT_EQ(0, aeron_cluster_session_manager_session_count(m_mgr));

    aeron_cluster_session_manager_restore_uncommitted_sessions(m_mgr, 300LL);

    /* Session at 100 < 300 was committed — freed, not in active list */
    EXPECT_EQ(0, aeron_cluster_session_manager_session_count(m_mgr));
    /* Ring should be drained */
    EXPECT_EQ(m_mgr->uncommitted_closed_head, m_mgr->uncommitted_closed_tail);
}

TEST_F(SessionManagerTest, restoreUncommittedSessionsMixedCommittedAndUncommitted)
{
    /* Session 210: committed (pos 100 <= commit 300), freed */
    make_uncommitted_session(m_mgr, 210, 100LL);
    /* Session 211: uncommitted (pos 500 > commit 300), restored */
    make_uncommitted_session(m_mgr, 211, 500LL);
    /* Session 212: uncommitted (pos 1000 > commit 300), restored */
    make_uncommitted_session(m_mgr, 212, 1000LL);
    ASSERT_EQ(0, aeron_cluster_session_manager_session_count(m_mgr));

    aeron_cluster_session_manager_restore_uncommitted_sessions(m_mgr, 300LL);

    EXPECT_EQ(2, aeron_cluster_session_manager_session_count(m_mgr));
    EXPECT_EQ(nullptr, aeron_cluster_session_manager_find(m_mgr, 210));
    EXPECT_NE(nullptr, aeron_cluster_session_manager_find(m_mgr, 211));
    EXPECT_NE(nullptr, aeron_cluster_session_manager_find(m_mgr, 212));
    EXPECT_EQ(m_mgr->uncommitted_closed_head, m_mgr->uncommitted_closed_tail);
}

/* Session 23: disconnect_sessions — smoke test with null publications (no real Aeron) */
TEST_F(SessionManagerTest, disconnectSessionsDoesNotCrashWithNullPublications)
{
    aeron_cluster_session_manager_on_replay_session_open(m_mgr, 0, 0, 300, 0, 1, "ch");
    auto *s = aeron_cluster_session_manager_find(m_mgr, 300);
    ASSERT_NE(nullptr, s);
    EXPECT_EQ(nullptr, s->response_publication);  /* no Aeron, so no pub */

    /* Should not crash even with NULL publications */
    aeron_cluster_session_manager_disconnect_sessions(m_mgr);
    EXPECT_EQ(1, aeron_cluster_session_manager_session_count(m_mgr));
}

/* -----------------------------------------------------------------------
 * SessionManager: STANDBY_SNAPSHOT action arm in process_pending_sessions
 * ----------------------------------------------------------------------- */
TEST_F(SessionManagerTest, standbySnapshotActionSessionIsRemovedAndFreed)
{
    /* Create a raw session with STANDBY_SNAPSHOT action and AUTHENTICATED state,
     * push it directly into pending_backup, then drive process_pending_sessions. */
    aeron_cluster_cluster_session_t *s = nullptr;
    ASSERT_EQ(0, aeron_cluster_cluster_session_create(
        &s, 999, 0, 1, "aeron:ipc", nullptr, 0, nullptr /* no aeron */));
    s->action = AERON_CLUSTER_SESSION_ACTION_STANDBY_SNAPSHOT;
    s->state  = AERON_CLUSTER_SESSION_STATE_AUTHENTICATED;

    /* Grow pending_backup and push */
    if (m_mgr->pending_backup_count >= m_mgr->pending_backup_capacity)
    {
        int new_cap = m_mgr->pending_backup_capacity + 8;
        m_mgr->pending_backup = static_cast<aeron_cluster_cluster_session_t **>(realloc(
            m_mgr->pending_backup,
            static_cast<size_t>(new_cap) * sizeof(aeron_cluster_cluster_session_t *)));
        m_mgr->pending_backup_capacity = new_cap;
    }
    m_mgr->pending_backup[m_mgr->pending_backup_count++] = s;
    ASSERT_EQ(1, m_mgr->pending_backup_count);

    /* process_pending_sessions should close and remove the STANDBY_SNAPSHOT session */
    int work = aeron_cluster_session_manager_process_pending_sessions(
        m_mgr, 0LL, 0LL, 0, 0LL);
    EXPECT_GT(work, 0);
    EXPECT_EQ(0, m_mgr->pending_backup_count);
}

/* -----------------------------------------------------------------------
 * SessionManager: max session limit enforcement
 * ----------------------------------------------------------------------- */
TEST_F(SessionManagerTest, shouldTrackSessionCountCorrectly)
{
    EXPECT_EQ(0, m_mgr->session_count);
    /* Creating sessions via the pending path and verifying count */
    aeron_cluster_cluster_session_t *s = nullptr;
    ASSERT_EQ(0, aeron_cluster_cluster_session_create(
        &s, 1000, 0, 1, "aeron:ipc", nullptr, 0, nullptr));
    s->state = AERON_CLUSTER_SESSION_STATE_OPEN;

    /* Add via pending_user (how sessions normally enter) */
    if (m_mgr->pending_user_count >= m_mgr->pending_user_capacity)
    {
        int new_cap = m_mgr->pending_user_capacity + 8;
        m_mgr->pending_user = static_cast<aeron_cluster_cluster_session_t **>(realloc(
            m_mgr->pending_user,
            static_cast<size_t>(new_cap) * sizeof(aeron_cluster_cluster_session_t *)));
        m_mgr->pending_user_capacity = new_cap;
    }
    m_mgr->pending_user[m_mgr->pending_user_count++] = s;
    EXPECT_EQ(1, m_mgr->pending_user_count);
}

/* =======================================================================
 * SessionManagerStandbySnapshotTest
 * Tests for processPendingStandbySnapshotNotifications commit-position gate.
 * ======================================================================= */

class SessionManagerStandbySnapshotTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        m_log_dir = make_test_dir("aeron_cluster_ssn_test_");
        aeron_delete_directory(m_log_dir.c_str());
        aeron_mkdir_recursive(m_log_dir.c_str(), 0777);
        ASSERT_EQ(0, aeron_cluster_recording_log_open(&m_log, m_log_dir.c_str(), true));
        ASSERT_EQ(0, aeron_cluster_session_manager_create(&m_manager, 1, nullptr));
        m_manager->recording_log = m_log;
    }

    void TearDown() override
    {
        m_manager->recording_log = nullptr;  /* log is closed separately */
        aeron_cluster_session_manager_close(m_manager);
        aeron_cluster_recording_log_close(m_log);
        aeron_delete_directory(m_log_dir.c_str());
    }

    /* Enqueue one batch with a single entry at the given logPosition */
    void enqueue_at(int64_t log_pos, int64_t recording_id = 99LL)
    {
        int64_t rec_ids[]          = { recording_id };
        int64_t term_ids[]         = { 0LL };
        int64_t term_base_pos[]    = { 0LL };
        int64_t log_positions[]    = { log_pos };
        int64_t timestamps[]       = { 1000LL };
        int32_t service_ids[]      = { 0 };
        aeron_cluster_session_manager_enqueue_standby_snapshot(
            m_manager, rec_ids, term_ids, term_base_pos,
            log_positions, timestamps, service_ids, 1);
    }

    /* Count STANDBY_SNAPSHOT entries in the recording log */
    int standby_snapshot_count()
    {
        /* Reload to pick up entries written via append_standby_snapshot */
        aeron_cluster_recording_log_reload(m_log);
        int count = 0;
        for (int i = 0; i < m_log->sorted_count; i++)
        {
            if (m_log->sorted_entries[i].entry_type ==
                AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_STANDBY_SNAPSHOT)
            {
                count++;
            }
        }
        return count;
    }

    std::string m_log_dir;
    aeron_cluster_recording_log_t   *m_log    = nullptr;
    aeron_cluster_session_manager_t *m_manager = nullptr;
};

TEST_F(SessionManagerStandbySnapshotTest, batchHeldUntilLogPositionCommitted)
{
    /* Enqueue a batch at logPosition=1024 */
    enqueue_at(1024LL);
    ASSERT_EQ(1, m_manager->pending_standby_count);

    /* Process with commitPosition=512 — not yet committed */
    int work = aeron_cluster_session_manager_process_pending_standby_snapshot_notifications(
        m_manager, 512LL, 0LL);
    EXPECT_EQ(0, work);
    EXPECT_EQ(1, m_manager->pending_standby_count);   /* still pending */
    EXPECT_EQ(0, standby_snapshot_count());            /* nothing written */
}

TEST_F(SessionManagerStandbySnapshotTest, batchReleasedWhenLogPositionCommitted)
{
    enqueue_at(1024LL);

    /* Process with commitPosition=1024 — exactly at logPosition */
    int work = aeron_cluster_session_manager_process_pending_standby_snapshot_notifications(
        m_manager, 1024LL, 0LL);
    EXPECT_EQ(1, work);
    EXPECT_EQ(0, m_manager->pending_standby_count);   /* queue drained */
    EXPECT_EQ(1, standby_snapshot_count());            /* written to log */
}

TEST_F(SessionManagerStandbySnapshotTest, multipleBatchesPartiallyReleased)
{
    enqueue_at(512LL,  10LL);   /* committed first */
    enqueue_at(2048LL, 20LL);   /* not yet committed */

    int work = aeron_cluster_session_manager_process_pending_standby_snapshot_notifications(
        m_manager, 512LL, 0LL);
    EXPECT_EQ(1, work);
    EXPECT_EQ(1, m_manager->pending_standby_count);   /* second batch still pending */
    EXPECT_EQ(1, standby_snapshot_count());

    /* Now commit the second batch */
    work = aeron_cluster_session_manager_process_pending_standby_snapshot_notifications(
        m_manager, 2048LL, 0LL);
    EXPECT_EQ(1, work);
    EXPECT_EQ(0, m_manager->pending_standby_count);
    EXPECT_EQ(2, standby_snapshot_count());
}

TEST_F(SessionManagerStandbySnapshotTest, delayedBatchHeldUntilDeadline)
{
    m_manager->standby_snapshot_notification_delay_ns = 1000LL;
    enqueue_at(100LL);

    /* logPosition committed but deadline not yet reached (nowNs=0) */
    int work = aeron_cluster_session_manager_process_pending_standby_snapshot_notifications(
        m_manager, 100LL, 0LL);
    EXPECT_EQ(0, work);
    EXPECT_EQ(1, m_manager->pending_standby_count);   /* held for delay */
    EXPECT_EQ(0, standby_snapshot_count());

    /* Advance clock past deadline */
    work = aeron_cluster_session_manager_process_pending_standby_snapshot_notifications(
        m_manager, 100LL, 1001LL);
    EXPECT_EQ(1, work);
    EXPECT_EQ(0, m_manager->pending_standby_count);
    EXPECT_EQ(1, standby_snapshot_count());
}

TEST_F(SessionManagerStandbySnapshotTest, emptyEnqueueIsNoop)
{
    aeron_cluster_session_manager_enqueue_standby_snapshot(
        m_manager, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, 0);
    EXPECT_EQ(0, m_manager->pending_standby_count);
}
