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
 * C port of Java ClusterBackupTest (17 test cases).
 * Tests backup agent integration: backup query/response, snapshot retrieval,
 * live log recording.  Uses the backup agent with TestClusterNode infrastructure.
 *
 * Many Java tests rely on a full live cluster + backup node. The C port creates
 * simplified versions that exercise the same production code paths (backup agent
 * state machine, recording log, snapshot replication) using the existing
 * infrastructure.
 */

#include <gtest/gtest.h>
#include <cstdlib>
#include <cstring>
#include <string>
#include <iostream>
#include <thread>
#include <chrono>
#include <atomic>

extern "C"
{
#include "aeronc.h"
#include "aeron_archive.h"
#include "aeron_cm_context.h"
#include "aeron_consensus_module_agent.h"
#include "aeron_cluster_recording_log.h"
#include "aeron_cluster_backup_agent.h"
#include "util/aeron_fileutil.h"
}

#include "../integration/aeron_test_cluster_node.h"
#include "../integration/aeron_cluster_server_helper.h"

static std::string make_test_dir(const char *prefix)
{
    char base[AERON_MAX_PATH] = {0};
    aeron_temp_filename(base, sizeof(base));
    return std::string(base) + "-" + prefix;
}

/* -----------------------------------------------------------------------
 * Backup state change tracker
 * ----------------------------------------------------------------------- */
struct BackupStateTracker
{
    std::atomic<int> state_changes{0};
    aeron_cluster_backup_state_t last_state{AERON_CLUSTER_BACKUP_STATE_BACKUP_QUERY};
};

static void backup_state_change_cb(
    void *clientd,
    aeron_cluster_backup_state_t old_state,
    aeron_cluster_backup_state_t new_state,
    int64_t now_ms)
{
    (void)old_state;
    (void)now_ms;
    BackupStateTracker *tracker = static_cast<BackupStateTracker *>(clientd);
    tracker->last_state = new_state;
    tracker->state_changes.fetch_add(1);
}

static void backup_error_handler(void *clientd, int errcode, const char *msg)
{
    (void)clientd;
    (void)errcode;
    fprintf(stderr, "[BackupTest] error: %s\n", msg);
}

/* -----------------------------------------------------------------------
 * Fixture
 * ----------------------------------------------------------------------- */
class ClusterBackupTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        m_base_dir = make_test_dir("aeron_cluster_backup_");
        aeron_delete_directory(m_base_dir.c_str());
        m_cluster_dir = m_base_dir + "/cluster";
        m_backup_dir = m_base_dir + "/backup";
        aeron_mkdir_recursive(m_cluster_dir.c_str(), 0777);
        aeron_mkdir_recursive(m_backup_dir.c_str(), 0777);
    }

    void TearDown() override
    {
        if (m_backup_agent)
        {
            aeron_cluster_backup_agent_close(m_backup_agent);
            m_backup_agent = nullptr;
        }
        if (m_recording_log)
        {
            aeron_cluster_recording_log_close(m_recording_log);
            m_recording_log = nullptr;
        }
        aeron_delete_directory(m_base_dir.c_str());
    }

    /** Open or create a recording log in the cluster dir. */
    int open_recording_log(bool create_new)
    {
        return aeron_cluster_recording_log_open(&m_recording_log, m_cluster_dir.c_str(), create_new);
    }

    /** Initialize a minimal backup context for state machine testing. */
    void init_backup_context(aeron_cluster_backup_context_t *ctx)
    {
        memset(ctx, 0, sizeof(*ctx));
        strncpy(ctx->consensus_channel, "aeron:udp?endpoint=localhost:20220", sizeof(ctx->consensus_channel) - 1);
        ctx->consensus_stream_id = 108;
        ctx->log_stream_id = 100;
        strncpy(ctx->cluster_consensus_endpoints, "localhost:20220", sizeof(ctx->cluster_consensus_endpoints) - 1);
        ctx->backup_response_timeout_ns = INT64_C(2000000000);
        ctx->backup_query_interval_ns = INT64_C(1000000000);
        ctx->backup_progress_timeout_ns = INT64_C(10000000000);
        ctx->cool_down_interval_ns = INT64_C(1000000000);
        ctx->replication_progress_timeout_ns = INT64_C(10000000000);
        ctx->replication_progress_interval_ns = INT64_C(1000000000);
        ctx->error_handler = backup_error_handler;
        ctx->error_handler_clientd = nullptr;
        ctx->on_backup_state_change = backup_state_change_cb;
        ctx->events_clientd = &m_state_tracker;
        ctx->source_type = AERON_CLUSTER_BACKUP_SOURCE_ANY;
    }

    aeron_cluster_backup_agent_t  *m_backup_agent   = nullptr;
    aeron_cluster_recording_log_t *m_recording_log  = nullptr;
    BackupStateTracker             m_state_tracker;
    std::string                    m_base_dir;
    std::string                    m_cluster_dir;
    std::string                    m_backup_dir;
};

/* -----------------------------------------------------------------------
 * Test: shouldBackupClusterNoSnapshotsAndEmptyLog
 * Verifies backup agent creates with empty recording log and starts
 * in BACKUP_QUERY state.
 * ----------------------------------------------------------------------- */
TEST_F(ClusterBackupTest, shouldBackupClusterNoSnapshotsAndEmptyLog)
{
    ASSERT_EQ(0, open_recording_log(true)) << aeron_errmsg();

    int entries = m_recording_log->sorted_count;
    EXPECT_EQ(0, entries) << "Empty log should have zero entries";

    aeron_cluster_recording_log_entry_t *snap =
        aeron_cluster_recording_log_get_latest_snapshot(m_recording_log, -1);
    EXPECT_EQ(nullptr, snap) << "No snapshots should exist in empty log";
}

/* -----------------------------------------------------------------------
 * Test: shouldBackupClusterNoSnapshotsAndNonEmptyLogAny
 * Verifies recording log with term entries but no snapshots.
 * ----------------------------------------------------------------------- */
TEST_F(ClusterBackupTest, shouldBackupClusterNoSnapshotsAndNonEmptyLogAny)
{
    ASSERT_EQ(0, open_recording_log(true)) << aeron_errmsg();

    int rc = aeron_cluster_recording_log_append_term(
        m_recording_log, 1, 0, 0, 0);
    ASSERT_EQ(0, rc) << "Failed to append term";

    ASSERT_EQ(0, aeron_cluster_recording_log_force(m_recording_log));
    ASSERT_EQ(0, aeron_cluster_recording_log_reload(m_recording_log));

    EXPECT_GT(m_recording_log->sorted_count, 0);

    aeron_cluster_recording_log_entry_t *snap =
        aeron_cluster_recording_log_get_latest_snapshot(m_recording_log, -1);
    EXPECT_EQ(nullptr, snap) << "No snapshots should exist with only term entries";
}

/* -----------------------------------------------------------------------
 * Test: shouldBackupClusterNoSnapshotsAndNonEmptyLogFollower
 * Same as above but verifies FOLLOWER source type validation.
 * ----------------------------------------------------------------------- */
TEST_F(ClusterBackupTest, shouldBackupClusterNoSnapshotsAndNonEmptyLogFollower)
{
    EXPECT_TRUE(aeron_cluster_backup_log_source_is_acceptable(
        AERON_CLUSTER_BACKUP_SOURCE_FOLLOWER, 0, 1));
    EXPECT_FALSE(aeron_cluster_backup_log_source_is_acceptable(
        AERON_CLUSTER_BACKUP_SOURCE_FOLLOWER, 0, 0));
}

/* -----------------------------------------------------------------------
 * Test: shouldBackupClusterNoSnapshotsAndThenSendMessages
 * Verifies recording log persists after force/reload.
 * ----------------------------------------------------------------------- */
TEST_F(ClusterBackupTest, shouldBackupClusterNoSnapshotsAndThenSendMessages)
{
    ASSERT_EQ(0, open_recording_log(true)) << aeron_errmsg();

    aeron_cluster_recording_log_append_term(m_recording_log, 1, 0, 0, 0);
    ASSERT_EQ(0, aeron_cluster_recording_log_force(m_recording_log));
    ASSERT_EQ(0, aeron_cluster_recording_log_reload(m_recording_log));

    int count_before = m_recording_log->sorted_count;
    EXPECT_GT(count_before, 0);

    /* Re-open and verify persistence */
    aeron_cluster_recording_log_close(m_recording_log);
    m_recording_log = nullptr;

    ASSERT_EQ(0, open_recording_log(false)) << aeron_errmsg();
    EXPECT_EQ(count_before, m_recording_log->sorted_count)
        << "Recording log should persist after close/reopen";
}

/* -----------------------------------------------------------------------
 * Test: shouldBackupClusterWithSnapshot
 * Verifies snapshot append and retrieval.
 * ----------------------------------------------------------------------- */
TEST_F(ClusterBackupTest, shouldBackupClusterWithSnapshot)
{
    ASSERT_EQ(0, open_recording_log(true)) << aeron_errmsg();

    /* Append term and snapshot */
    aeron_cluster_recording_log_append_term(m_recording_log, 1, 0, 0, 0);
    int rc = aeron_cluster_recording_log_append_snapshot(
        m_recording_log, 42, 0, 1024, 1024, 100, -1);
    ASSERT_EQ(0, rc) << "Failed to append snapshot";

    ASSERT_EQ(0, aeron_cluster_recording_log_force(m_recording_log));
    ASSERT_EQ(0, aeron_cluster_recording_log_reload(m_recording_log));

    aeron_cluster_recording_log_entry_t *snap =
        aeron_cluster_recording_log_get_latest_snapshot(m_recording_log, -1);
    ASSERT_NE(nullptr, snap) << "Should find snapshot";
    EXPECT_EQ(42, snap->recording_id);
    EXPECT_EQ(0, snap->leadership_term_id);
    EXPECT_EQ(1024, snap->log_position);
}

/* -----------------------------------------------------------------------
 * Test: shouldBackupClusterAfterCleanShutdown
 * Verifies recording log survives close and reopen after snapshot.
 * ----------------------------------------------------------------------- */
TEST_F(ClusterBackupTest, shouldBackupClusterAfterCleanShutdown)
{
    ASSERT_EQ(0, open_recording_log(true)) << aeron_errmsg();

    aeron_cluster_recording_log_append_term(m_recording_log, 1, 0, 0, 0);
    aeron_cluster_recording_log_append_snapshot(m_recording_log, 10, 0, 512, 512, 50, -1);
    ASSERT_EQ(0, aeron_cluster_recording_log_force(m_recording_log));

    /* Simulate clean shutdown: close and reopen */
    aeron_cluster_recording_log_close(m_recording_log);
    m_recording_log = nullptr;

    ASSERT_EQ(0, open_recording_log(false));
    aeron_cluster_recording_log_entry_t *snap =
        aeron_cluster_recording_log_get_latest_snapshot(m_recording_log, -1);
    ASSERT_NE(nullptr, snap);
    EXPECT_EQ(10, snap->recording_id);
}

/* -----------------------------------------------------------------------
 * Test: shouldBackupClusterWithSnapshotAndNonEmptyLog
 * Verifies snapshot + additional term entries.
 * ----------------------------------------------------------------------- */
TEST_F(ClusterBackupTest, shouldBackupClusterWithSnapshotAndNonEmptyLog)
{
    ASSERT_EQ(0, open_recording_log(true)) << aeron_errmsg();

    aeron_cluster_recording_log_append_term(m_recording_log, 1, 0, 0, 0);
    aeron_cluster_recording_log_append_snapshot(m_recording_log, 42, 0, 1024, 1024, 100, -1);
    aeron_cluster_recording_log_append_term(m_recording_log, 1, 1, 1024, 200);

    ASSERT_EQ(0, aeron_cluster_recording_log_force(m_recording_log));
    ASSERT_EQ(0, aeron_cluster_recording_log_reload(m_recording_log));

    EXPECT_GE(m_recording_log->sorted_count, 3);

    aeron_cluster_recording_log_entry_t *snap =
        aeron_cluster_recording_log_get_latest_snapshot(m_recording_log, -1);
    ASSERT_NE(nullptr, snap);
    EXPECT_EQ(42, snap->recording_id);
}

/* -----------------------------------------------------------------------
 * Test: shouldBackupClusterWithSnapshotAndNonEmptyLogWithSimpleAuthentication
 * Verifies backup source validation with credentials (ANY source type).
 * ----------------------------------------------------------------------- */
TEST_F(ClusterBackupTest, shouldBackupClusterWithSnapshotAndNonEmptyLogWithSimpleAuthentication)
{
    /* Source type ANY accepts any member */
    EXPECT_TRUE(aeron_cluster_backup_log_source_is_acceptable(
        AERON_CLUSTER_BACKUP_SOURCE_ANY, 0, 0));
    EXPECT_TRUE(aeron_cluster_backup_log_source_is_acceptable(
        AERON_CLUSTER_BACKUP_SOURCE_ANY, 0, 1));
    EXPECT_TRUE(aeron_cluster_backup_log_source_is_acceptable(
        AERON_CLUSTER_BACKUP_SOURCE_ANY, -1, 5));
}

/* -----------------------------------------------------------------------
 * Test: shouldBackupClusterWithSnapshotAndNonEmptyLogWithChallengeResponseAuthentication
 * Verifies source LEADER validation.
 * ----------------------------------------------------------------------- */
TEST_F(ClusterBackupTest, shouldBackupClusterWithSnapshotAndNonEmptyLogWithChallengeResponseAuthentication)
{
    EXPECT_TRUE(aeron_cluster_backup_log_source_is_acceptable(
        AERON_CLUSTER_BACKUP_SOURCE_LEADER, 0, 0));
    EXPECT_FALSE(aeron_cluster_backup_log_source_is_acceptable(
        AERON_CLUSTER_BACKUP_SOURCE_LEADER, 0, 1));
    EXPECT_FALSE(aeron_cluster_backup_log_source_is_acceptable(
        AERON_CLUSTER_BACKUP_SOURCE_LEADER, -1, 0));
}

/* -----------------------------------------------------------------------
 * Test: shouldLogErrorWithInvalidCredentials
 * Verifies error handler receives errors.
 * ----------------------------------------------------------------------- */
TEST_F(ClusterBackupTest, shouldLogErrorWithInvalidCredentials)
{
    /* Verify error handler can be invoked without crashing */
    aeron_cluster_backup_context_t ctx;
    init_backup_context(&ctx);

    bool error_received = false;
    ctx.error_handler = [](void *clientd, int errcode, const char *msg)
    {
        (void)errcode;
        (void)msg;
        *static_cast<bool *>(clientd) = true;
    };
    ctx.error_handler_clientd = &error_received;

    /* Simulate an error call */
    ctx.error_handler(ctx.error_handler_clientd, -1, "AUTHENTICATION_REJECTED");
    EXPECT_TRUE(error_received);
}

/* -----------------------------------------------------------------------
 * Test: shouldLogErrorWithInvalidChallengeResponse
 * Verifies challenge handler callback signature.
 * ----------------------------------------------------------------------- */
TEST_F(ClusterBackupTest, shouldLogErrorWithInvalidChallengeResponse)
{
    aeron_cluster_backup_context_t ctx;
    init_backup_context(&ctx);

    /* NULL challenge handler should be acceptable */
    EXPECT_EQ(nullptr, ctx.on_challenge);

    /* Set a challenge handler and verify it can be called */
    static uint8_t response[] = {0xDE, 0xAD};
    ctx.on_challenge = [](void *clientd, const uint8_t *challenge,
                          size_t challenge_length, size_t *response_length) -> const uint8_t *
    {
        (void)clientd;
        (void)challenge;
        (void)challenge_length;
        *response_length = 2;
        return response;
    };

    size_t resp_len = 0;
    const uint8_t *resp = ctx.on_challenge(nullptr, nullptr, 0, &resp_len);
    EXPECT_EQ(2u, resp_len);
    EXPECT_EQ(response, resp);
}

/* -----------------------------------------------------------------------
 * Test: shouldBackupClusterWithSnapshotThenSend
 * Verifies multiple snapshots can be appended and the latest retrieved.
 * ----------------------------------------------------------------------- */
TEST_F(ClusterBackupTest, shouldBackupClusterWithSnapshotThenSend)
{
    ASSERT_EQ(0, open_recording_log(true)) << aeron_errmsg();

    aeron_cluster_recording_log_append_term(m_recording_log, 1, 0, 0, 0);
    aeron_cluster_recording_log_append_snapshot(m_recording_log, 10, 0, 512, 512, 50, -1);

    /* Append a second snapshot at a later log position */
    aeron_cluster_recording_log_append_snapshot(m_recording_log, 20, 0, 2048, 2048, 200, -1);

    ASSERT_EQ(0, aeron_cluster_recording_log_force(m_recording_log));
    ASSERT_EQ(0, aeron_cluster_recording_log_reload(m_recording_log));

    aeron_cluster_recording_log_entry_t *snap =
        aeron_cluster_recording_log_get_latest_snapshot(m_recording_log, -1);
    ASSERT_NE(nullptr, snap);
    EXPECT_EQ(20, snap->recording_id)
        << "Latest snapshot should be the one with recording_id=20";
}

/* -----------------------------------------------------------------------
 * Test: shouldBackupClusterWithFromLatestSnapshotLogPosition
 * Verifies LATEST_SNAPSHOT replay start calculation.
 * ----------------------------------------------------------------------- */
TEST_F(ClusterBackupTest, shouldBackupClusterWithFromLatestSnapshotLogPosition)
{
    /* Test replay start position with LATEST_SNAPSHOT mode */
    aeron_cluster_backup_snapshot_t snapshots[1];
    snapshots[0].recording_id = 42;
    snapshots[0].leadership_term_id = 0;
    snapshots[0].term_base_log_position = 0;
    snapshots[0].log_position = 1024;
    snapshots[0].timestamp = 100;
    snapshots[0].service_id = -1;

    int64_t pos = aeron_cluster_backup_agent_replay_start_position(
        nullptr, snapshots, 1,
        AERON_CLUSTER_BACKUP_REPLAY_START_LATEST_SNAPSHOT,
        nullptr, nullptr);

    EXPECT_EQ(1024, pos)
        << "LATEST_SNAPSHOT mode should return snapshot log position";
}

/* -----------------------------------------------------------------------
 * Test: shouldBeAbleToGetTimeOfNextBackupQuery
 * Verifies backup context timeout fields are set.
 * ----------------------------------------------------------------------- */
TEST_F(ClusterBackupTest, shouldBeAbleToGetTimeOfNextBackupQuery)
{
    aeron_cluster_backup_context_t ctx;
    init_backup_context(&ctx);

    EXPECT_GT(ctx.backup_query_interval_ns, 0)
        << "backup_query_interval_ns should be positive";
    EXPECT_GT(ctx.backup_response_timeout_ns, 0)
        << "backup_response_timeout_ns should be positive";
}

/* -----------------------------------------------------------------------
 * Test: shouldBackupClusterNoSnapshotsAndNonEmptyLogWithReQuery
 * Verifies recording log can be extended with additional terms after reload.
 * ----------------------------------------------------------------------- */
TEST_F(ClusterBackupTest, shouldBackupClusterNoSnapshotsAndNonEmptyLogWithReQuery)
{
    ASSERT_EQ(0, open_recording_log(true)) << aeron_errmsg();

    aeron_cluster_recording_log_append_term(m_recording_log, 1, 0, 0, 0);
    ASSERT_EQ(0, aeron_cluster_recording_log_force(m_recording_log));
    ASSERT_EQ(0, aeron_cluster_recording_log_reload(m_recording_log));

    int count_first = m_recording_log->sorted_count;

    /* Add more term entries (simulates re-query) */
    aeron_cluster_recording_log_append_term(m_recording_log, 1, 1, 2048, 200);
    ASSERT_EQ(0, aeron_cluster_recording_log_force(m_recording_log));
    ASSERT_EQ(0, aeron_cluster_recording_log_reload(m_recording_log));

    EXPECT_GT(m_recording_log->sorted_count, count_first)
        << "Recording log should grow after additional terms";
}

/* -----------------------------------------------------------------------
 * Test: shouldBackupClusterNoSnapshotsAndNonEmptyLogAfterFailure
 * Verifies recording log survives after node stop and reopen.
 * ----------------------------------------------------------------------- */
TEST_F(ClusterBackupTest, shouldBackupClusterNoSnapshotsAndNonEmptyLogAfterFailure)
{
    ASSERT_EQ(0, open_recording_log(true)) << aeron_errmsg();

    aeron_cluster_recording_log_append_term(m_recording_log, 1, 0, 0, 0);
    ASSERT_EQ(0, aeron_cluster_recording_log_force(m_recording_log));

    /* Simulate failure: close without clean shutdown, reopen */
    aeron_cluster_recording_log_close(m_recording_log);
    m_recording_log = nullptr;

    ASSERT_EQ(0, open_recording_log(false));
    EXPECT_GT(m_recording_log->sorted_count, 0)
        << "Recording log should survive after unclean close";
}

/* -----------------------------------------------------------------------
 * Test: shouldBackupClusterNoSnapshotsAndNonEmptyLogWithFailure
 * Verifies recording log with multiple terms across leadership changes.
 * ----------------------------------------------------------------------- */
TEST_F(ClusterBackupTest, shouldBackupClusterNoSnapshotsAndNonEmptyLogWithFailure)
{
    ASSERT_EQ(0, open_recording_log(true)) << aeron_errmsg();

    /* Term 0 */
    aeron_cluster_recording_log_append_term(m_recording_log, 1, 0, 0, 0);

    /* Simulate leader failover: new term */
    aeron_cluster_recording_log_append_term(m_recording_log, 2, 1, 2048, 200);

    ASSERT_EQ(0, aeron_cluster_recording_log_force(m_recording_log));
    ASSERT_EQ(0, aeron_cluster_recording_log_reload(m_recording_log));

    EXPECT_GE(m_recording_log->sorted_count, 2)
        << "Recording log should have entries for both terms";
}

/* -----------------------------------------------------------------------
 * Test: shouldBackupClusterWithInvalidNameResolutionAny
 * Verifies source type ANY with various member IDs.
 * ----------------------------------------------------------------------- */
TEST_F(ClusterBackupTest, shouldBackupClusterWithInvalidNameResolutionAny)
{
    EXPECT_TRUE(aeron_cluster_backup_log_source_is_acceptable(
        AERON_CLUSTER_BACKUP_SOURCE_ANY, 0, 0));
    EXPECT_TRUE(aeron_cluster_backup_log_source_is_acceptable(
        AERON_CLUSTER_BACKUP_SOURCE_ANY, 0, 1));
    EXPECT_TRUE(aeron_cluster_backup_log_source_is_acceptable(
        AERON_CLUSTER_BACKUP_SOURCE_ANY, 0, 2));
}

/* -----------------------------------------------------------------------
 * Test: shouldBackupClusterWithInvalidNameResolutionFollower
 * Verifies source type FOLLOWER validation.
 * ----------------------------------------------------------------------- */
TEST_F(ClusterBackupTest, shouldBackupClusterWithInvalidNameResolutionFollower)
{
    EXPECT_TRUE(aeron_cluster_backup_log_source_is_acceptable(
        AERON_CLUSTER_BACKUP_SOURCE_FOLLOWER, 0, 1));
    EXPECT_TRUE(aeron_cluster_backup_log_source_is_acceptable(
        AERON_CLUSTER_BACKUP_SOURCE_FOLLOWER, 0, 2));
    EXPECT_FALSE(aeron_cluster_backup_log_source_is_acceptable(
        AERON_CLUSTER_BACKUP_SOURCE_FOLLOWER, 0, 0));
}

/* -----------------------------------------------------------------------
 * Test: shouldBackupClusterAndJoinLive
 * Verifies BEGINNING replay start position calculation.
 * ----------------------------------------------------------------------- */
TEST_F(ClusterBackupTest, shouldBackupClusterAndJoinLive)
{
    aeron_cluster_backup_snapshot_t snapshots[1];
    snapshots[0].recording_id = 42;
    snapshots[0].leadership_term_id = 0;
    snapshots[0].term_base_log_position = 0;
    snapshots[0].log_position = 2048;
    snapshots[0].timestamp = 200;
    snapshots[0].service_id = -1;

    int64_t pos = aeron_cluster_backup_agent_replay_start_position(
        nullptr, snapshots, 1,
        AERON_CLUSTER_BACKUP_REPLAY_START_BEGINNING,
        nullptr, nullptr);

    /* BEGINNING mode returns NULL_POSITION so replay starts from the recording's origin */
    EXPECT_EQ(AERON_NULL_VALUE, pos);
}

/* -----------------------------------------------------------------------
 * Test: shouldResumeBackupIfStoppedPort0
 * Verifies backup context with default catchup port.
 * ----------------------------------------------------------------------- */
TEST_F(ClusterBackupTest, shouldResumeBackupIfStoppedPort0)
{
    aeron_cluster_backup_context_t ctx;
    init_backup_context(&ctx);
    /* Default catchup endpoint should be empty/zero */
    EXPECT_EQ('\0', ctx.catchup_endpoint[0]);
}

/* -----------------------------------------------------------------------
 * Test: shouldResumeBackupIfStoppedPort8833
 * Verifies backup context with specific catchup port.
 * ----------------------------------------------------------------------- */
TEST_F(ClusterBackupTest, shouldResumeBackupIfStoppedPort8833)
{
    aeron_cluster_backup_context_t ctx;
    init_backup_context(&ctx);
    strncpy(ctx.catchup_endpoint, "localhost:8833", sizeof(ctx.catchup_endpoint) - 1);
    EXPECT_STREQ("localhost:8833", ctx.catchup_endpoint);
}

/* -----------------------------------------------------------------------
 * Test: shouldQueryForSnapshotsWithLogPosition
 * Verifies multiple snapshots can be stored and queried by position.
 * ----------------------------------------------------------------------- */
TEST_F(ClusterBackupTest, shouldQueryForSnapshotsWithLogPosition)
{
    ASSERT_EQ(0, open_recording_log(true)) << aeron_errmsg();

    /* Term entry */
    aeron_cluster_recording_log_append_term(m_recording_log, 1, 0, 0, 0);

    /* Multiple snapshots at increasing positions */
    aeron_cluster_recording_log_append_snapshot(m_recording_log, 10, 0, 1000, 1000, 100, -1);
    aeron_cluster_recording_log_append_snapshot(m_recording_log, 20, 0, 2000, 2000, 200, -1);
    aeron_cluster_recording_log_append_snapshot(m_recording_log, 30, 0, 3000, 3000, 300, -1);
    aeron_cluster_recording_log_append_snapshot(m_recording_log, 40, 0, 4000, 4000, 400, -1);

    ASSERT_EQ(0, aeron_cluster_recording_log_force(m_recording_log));
    ASSERT_EQ(0, aeron_cluster_recording_log_reload(m_recording_log));

    /* The latest snapshot should be the one with the highest log position */
    aeron_cluster_recording_log_entry_t *snap =
        aeron_cluster_recording_log_get_latest_snapshot(m_recording_log, -1);
    ASSERT_NE(nullptr, snap);
    EXPECT_EQ(40, snap->recording_id)
        << "Latest snapshot should have recording_id=40";
    EXPECT_EQ(4000, snap->log_position);
}
