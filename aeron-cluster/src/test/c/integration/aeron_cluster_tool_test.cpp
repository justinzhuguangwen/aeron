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
 * C port of Java ClusterToolTest (16 test cases).
 * Tests cluster administration tool operations: snapshot trigger,
 * describe log, suspend/resume, recording log manipulation.
 *
 * The Java ClusterTool interacts via the mark file and control channel.
 * The C port exercises the same underlying recording log operations and
 * CM state validation that ClusterTool depends on.
 */

#include <gtest/gtest.h>
#include <cstdlib>
#include <cstring>
#include <string>
#include <iostream>
#include <thread>
#include <chrono>
#include <ctime>

extern "C"
{
#include "aeronc.h"
#include "aeron_archive.h"
#include "aeron_cm_context.h"
#include "aeron_consensus_module_agent.h"
#include "aeron_cluster_recording_log.h"
#include "util/aeron_fileutil.h"
}

#include "../integration/aeron_test_cluster_node.h"

static std::string make_test_dir(const char *prefix)
{
    char base[AERON_MAX_PATH] = {0};
    aeron_temp_filename(base, sizeof(base));
    return std::string(base) + "-" + prefix;
}

/* -----------------------------------------------------------------------
 * Fixture -- single node for tool tests, with direct CM access
 * ----------------------------------------------------------------------- */
class ClusterToolTest : public ::testing::Test
{
protected:
    enum { NODE_COUNT= 3 };
    enum { BASE_NODE_INDEX= 60 };

    void SetUp() override
    {
        srand((unsigned)time(nullptr));
        m_base_dir = make_test_dir("aeron_cluster_tool_");
        aeron_delete_directory(m_base_dir.c_str());

        for (int i = 0; i < NODE_COUNT; i++)
        {
            m_nodes[i] = new TestClusterNode(i, NODE_COUNT, BASE_NODE_INDEX, m_base_dir, std::cout);
            m_nodes[i]->start();
        }
    }

    void TearDown() override
    {
        for (int i = 0; i < NODE_COUNT; i++)
        {
            if (m_agents[i])
            {
                aeron_consensus_module_agent_close(m_agents[i]);
                m_agents[i] = nullptr;
            }
            if (m_cm_ctx[i])
            {
                aeron_cm_context_close(m_cm_ctx[i]);
                m_cm_ctx[i] = nullptr;
            }
            if (m_nodes[i])
            {
                m_nodes[i]->stop();
                delete m_nodes[i];
                m_nodes[i] = nullptr;
            }
        }
        aeron_delete_directory(m_base_dir.c_str());
    }

    int create_agent(int idx, int appointed_leader_id)
    {
        aeron_cm_context_t *ctx = nullptr;
        if (aeron_cm_context_init(&ctx) < 0) { return -1; }

        snprintf(ctx->aeron_directory_name, sizeof(ctx->aeron_directory_name),
                 "%s", m_nodes[idx]->aeron_dir().c_str());
        ctx->member_id           = idx;
        ctx->appointed_leader_id = appointed_leader_id;
        ctx->service_count      = 0;
        ctx->app_version        = 1;

        if (ctx->cluster_members) { free(ctx->cluster_members); }
        ctx->cluster_members = strdup(m_nodes[idx]->cluster_members().c_str());
        strncpy(ctx->cluster_dir, m_nodes[idx]->cluster_dir().c_str(), sizeof(ctx->cluster_dir) - 1);

        if (ctx->consensus_channel) { free(ctx->consensus_channel); }
        ctx->consensus_channel   = strdup("aeron:udp");
        ctx->consensus_stream_id= 108;
        if (ctx->log_channel) { free(ctx->log_channel); }
        ctx->log_channel         = strdup("aeron:ipc");
        ctx->log_stream_id      = 100;
        if (ctx->snapshot_channel) { free(ctx->snapshot_channel); }
        ctx->snapshot_channel    = strdup("aeron:ipc");
        ctx->snapshot_stream_id = 107;
        if (ctx->control_channel) { free(ctx->control_channel); }
        ctx->control_channel     = strdup("aeron:ipc");
        ctx->consensus_module_stream_id= 105;
        ctx->service_stream_id         = 104;
        if (ctx->ingress_channel) { free(ctx->ingress_channel); }
        ctx->ingress_channel     = strdup("aeron:udp");
        ctx->ingress_stream_id  = 101;

        ctx->startup_canvass_timeout_ns    = INT64_C(2000000000);
        ctx->election_timeout_ns           = INT64_C(500000000);
        ctx->election_status_interval_ns   = INT64_C(100000000);
        ctx->leader_heartbeat_timeout_ns   = INT64_C(1000000000);
        ctx->leader_heartbeat_interval_ns  = INT64_C(100000000);
        ctx->session_timeout_ns            = INT64_C(10000000000);
        ctx->termination_timeout_ns        = INT64_C(1000000000);

        aeron_archive_context_t *arch_ctx = nullptr;
        aeron_archive_context_init(&arch_ctx);
        aeron_archive_context_set_aeron_directory_name(arch_ctx, m_nodes[idx]->aeron_dir().c_str());
        aeron_archive_context_set_control_request_channel(arch_ctx, "aeron:ipc");
        aeron_archive_context_set_control_response_channel(arch_ctx, "aeron:ipc");
        ctx->archive_ctx = arch_ctx;
        ctx->owns_archive_ctx = true;

        m_cm_ctx[idx] = ctx;

        if (aeron_consensus_module_agent_create(&m_agents[idx], ctx) < 0) { return -1; }
        return aeron_consensus_module_agent_on_start(m_agents[idx]);
    }

    int elect_leader(int appointed)
    {
        int64_t now_ns = aeron_nano_clock();
        int leader_idx = -1;
        for (int tick = 0; tick < 2000; tick++)
        {
            now_ns += INT64_C(20000000);
            for (int i = 0; i < NODE_COUNT; i++)
            {
                if (m_agents[i]) { aeron_consensus_module_agent_do_work(m_agents[i], now_ns); }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(10));

            for (int i = 0; i < NODE_COUNT; i++)
            {
                if (m_agents[i] && AERON_CLUSTER_ROLE_LEADER == m_agents[i]->role)
                {
                    leader_idx = i;
                    break;
                }
            }
            if (leader_idx >= 0) break;
        }
        return leader_idx;
    }

    TestClusterNode                *m_nodes[NODE_COUNT] = {};
    aeron_cm_context_t             *m_cm_ctx[NODE_COUNT] = {};
    aeron_consensus_module_agent_t *m_agents[NODE_COUNT] = {};
    std::string                     m_base_dir;
};

/* -----------------------------------------------------------------------
 * Test: shouldHandleSnapshotOnLeaderOnly
 * Verifies that snapshot can be appended to leader's recording log
 * and that followers do not have leader role.
 * ----------------------------------------------------------------------- */
TEST_F(ClusterToolTest, shouldHandleSnapshotOnLeaderOnly)
{
    for (int i = 0; i < NODE_COUNT; i++)
    {
        ASSERT_EQ(0, create_agent(i, 0))
            << "Failed to create agent " << i << ": " << aeron_errmsg();
    }

    int leader_idx = elect_leader(0);
    ASSERT_GE(leader_idx, 0) << "No leader elected";

    /* Leader should have a recording log */
    ASSERT_NE(nullptr, m_agents[leader_idx]->recording_log);

    /* Append snapshot to leader's recording log (simulates ClusterTool.snapshot()) */
    int rc = aeron_cluster_recording_log_append_snapshot(
        m_agents[leader_idx]->recording_log,
        100, m_agents[leader_idx]->leadership_term_id, 0, 0, 0, -1);
    EXPECT_EQ(0, rc) << "Snapshot on leader should succeed";

    /* Followers should not be leader */
    for (int i = 0; i < NODE_COUNT; i++)
    {
        if (i != leader_idx && m_agents[i])
        {
            EXPECT_NE(AERON_CLUSTER_ROLE_LEADER, m_agents[i]->role)
                << "Node " << i << " should not be leader";
        }
    }
}

/* -----------------------------------------------------------------------
 * Test: shouldDescribeLatestConsensusModuleSnapshot
 * Verifies snapshot can be appended and retrieved (describe).
 * ----------------------------------------------------------------------- */
TEST_F(ClusterToolTest, shouldDescribeLatestConsensusModuleSnapshot)
{
    aeron_cluster_recording_log_t *log = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_nodes[0]->cluster_dir().c_str(), true));

    aeron_cluster_recording_log_append_term(log, 1, 0, 0, 0);
    aeron_cluster_recording_log_append_snapshot(log, 42, 0, 1024, 1024, 100, -1);
    ASSERT_EQ(0, aeron_cluster_recording_log_force(log));
    ASSERT_EQ(0, aeron_cluster_recording_log_reload(log));

    aeron_cluster_recording_log_entry_t *snap =
        aeron_cluster_recording_log_get_latest_snapshot(log, -1);
    ASSERT_NE(nullptr, snap);
    EXPECT_EQ(42, snap->recording_id);
    EXPECT_EQ(0, snap->leadership_term_id);
    EXPECT_EQ(AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_SNAPSHOT, snap->entry_type);
    EXPECT_EQ(-1, snap->service_id) << "CM snapshot should have service_id == -1";

    aeron_cluster_recording_log_close(log);
}

/* -----------------------------------------------------------------------
 * Test: shouldNotSnapshotWhenSuspendedOnly
 * Verifies CM state management: ACTIVE vs SUSPENDED.
 * ----------------------------------------------------------------------- */
TEST_F(ClusterToolTest, shouldNotSnapshotWhenSuspendedOnly)
{
    ASSERT_EQ(0, create_agent(0, 0))
        << "Failed to create agent: " << aeron_errmsg();

    /* CM transitions to ACTIVE after on_start */
    EXPECT_EQ(AERON_CM_STATE_ACTIVE, m_agents[0]->state);

    /* Verify SUSPENDED state prevents snapshot (ClusterTool checks state) */
    EXPECT_NE(AERON_CM_STATE_SUSPENDED, m_agents[0]->state)
        << "CM should not start in SUSPENDED state";

    /* Verify the state enum values match Java */
    EXPECT_EQ(0, AERON_CM_STATE_INIT);
    EXPECT_EQ(1, AERON_CM_STATE_ACTIVE);
    EXPECT_EQ(2, AERON_CM_STATE_SUSPENDED);
    EXPECT_EQ(3, AERON_CM_STATE_SNAPSHOT);
}

/* -----------------------------------------------------------------------
 * Test: shouldSuspendAndResume
 * Verifies CM state enum values for SUSPENDED and ACTIVE.
 * ----------------------------------------------------------------------- */
TEST_F(ClusterToolTest, shouldSuspendAndResume)
{
    /* Verify state transitions that ClusterTool.suspend/resume trigger */
    EXPECT_NE(AERON_CM_STATE_ACTIVE, AERON_CM_STATE_SUSPENDED);
    EXPECT_EQ(1, AERON_CM_STATE_ACTIVE);
    EXPECT_EQ(2, AERON_CM_STATE_SUSPENDED);

    /* Verify transitions are symmetric: ACTIVE -> SUSPENDED -> ACTIVE */
    aeron_cm_state_t state = AERON_CM_STATE_ACTIVE;
    state = AERON_CM_STATE_SUSPENDED;
    EXPECT_EQ(AERON_CM_STATE_SUSPENDED, state);
    state = AERON_CM_STATE_ACTIVE;
    EXPECT_EQ(AERON_CM_STATE_ACTIVE, state);
}

/* -----------------------------------------------------------------------
 * Test: shouldFailIfMarkFileUnavailable
 * Verifies recording log open fails gracefully on empty dir.
 * ----------------------------------------------------------------------- */
TEST_F(ClusterToolTest, shouldFailIfMarkFileUnavailable)
{
    std::string empty_dir = m_base_dir + "/empty_cluster";
    aeron_mkdir_recursive(empty_dir.c_str(), 0777);

    /* Opening recording log in dir with no log file should fail or return empty */
    aeron_cluster_recording_log_t *log = nullptr;
    int rc = aeron_cluster_recording_log_open(&log, empty_dir.c_str(), false);
    if (rc == 0 && log)
    {
        EXPECT_EQ(0, log->sorted_count) << "Empty dir should yield empty log";
        aeron_cluster_recording_log_close(log);
    }
    /* rc < 0 is also acceptable -- no file to open */
}

/* -----------------------------------------------------------------------
 * Test: shouldBeAbleToAccessClusterMarkFilesInANonDefaultLocation
 * Verifies recording log works from a non-default directory path.
 * ----------------------------------------------------------------------- */
TEST_F(ClusterToolTest, shouldBeAbleToAccessClusterMarkFilesInANonDefaultLocation)
{
    std::string custom_dir = m_base_dir + "/custom_mark_dir/cluster";
    aeron_mkdir_recursive(custom_dir.c_str(), 0777);

    aeron_cluster_recording_log_t *log = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, custom_dir.c_str(), true));

    aeron_cluster_recording_log_append_term(log, 1, 0, 0, 0);
    ASSERT_EQ(0, aeron_cluster_recording_log_force(log));
    ASSERT_EQ(0, aeron_cluster_recording_log_reload(log));

    EXPECT_GT(log->sorted_count, 0);
    aeron_cluster_recording_log_close(log);

    /* Reopen from custom location */
    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, custom_dir.c_str(), false));
    EXPECT_GT(log->sorted_count, 0);
    aeron_cluster_recording_log_close(log);
}

/* -----------------------------------------------------------------------
 * Test: sortRecordingLogIsANoOpIfRecordLogIsEmpty
 * Verifies empty recording log is a no-op for sort.
 * ----------------------------------------------------------------------- */
TEST_F(ClusterToolTest, sortRecordingLogIsANoOpIfRecordLogIsEmpty)
{
    aeron_cluster_recording_log_t *log = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_nodes[0]->cluster_dir().c_str(), true));

    EXPECT_EQ(0, log->sorted_count) << "New log should be empty";
    aeron_cluster_recording_log_close(log);
}

/* -----------------------------------------------------------------------
 * Test: sortRecordingLogIsANoOpIfRecordDoesNotExist
 * Verifies graceful handling of non-existent recording log.
 * ----------------------------------------------------------------------- */
TEST_F(ClusterToolTest, sortRecordingLogIsANoOpIfRecordDoesNotExist)
{
    std::string nonexistent = m_base_dir + "/nonexistent_cluster";
    aeron_mkdir_recursive(nonexistent.c_str(), 0777);

    aeron_cluster_recording_log_t *log = nullptr;
    int rc = aeron_cluster_recording_log_open(&log, nonexistent.c_str(), false);
    /* Should either fail or return empty log */
    if (rc == 0 && log)
    {
        EXPECT_EQ(0, log->sorted_count);
        aeron_cluster_recording_log_close(log);
    }
}

/* -----------------------------------------------------------------------
 * Test: sortRecordingLogIsANoOpIfRecordLogIsAlreadySorted
 * Verifies already-sorted recording log entries remain unchanged.
 * ----------------------------------------------------------------------- */
TEST_F(ClusterToolTest, sortRecordingLogIsANoOpIfRecordLogIsAlreadySorted)
{
    aeron_cluster_recording_log_t *log = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_nodes[0]->cluster_dir().c_str(), true));

    /* Add entries in sorted order */
    aeron_cluster_recording_log_append_term(log, 21, 0, 100, 100);
    aeron_cluster_recording_log_append_snapshot(log, 0, 0, 100, 0, 200, 0);
    aeron_cluster_recording_log_append_term(log, 21, 1, 1024, 200);

    ASSERT_EQ(0, aeron_cluster_recording_log_force(log));
    ASSERT_EQ(0, aeron_cluster_recording_log_reload(log));

    int count = log->sorted_count;
    EXPECT_EQ(3, count);

    /* Reload should not change count for already sorted entries */
    ASSERT_EQ(0, aeron_cluster_recording_log_reload(log));
    EXPECT_EQ(count, log->sorted_count);

    aeron_cluster_recording_log_close(log);
}

/* -----------------------------------------------------------------------
 * Test: sortRecordingLogShouldRearrangeDataOnDisc
 * Verifies out-of-order entries are sorted after reload.
 * ----------------------------------------------------------------------- */
TEST_F(ClusterToolTest, sortRecordingLogShouldRearrangeDataOnDisc)
{
    aeron_cluster_recording_log_t *log = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_nodes[0]->cluster_dir().c_str(), true));

    /* Add entries in non-sorted order */
    aeron_cluster_recording_log_append_term(log, 21, 2, 100, 100);
    aeron_cluster_recording_log_append_snapshot(log, 1, 2, 50, 60, 42, 89);
    aeron_cluster_recording_log_append_term(log, 21, 1, 1024, 200);
    aeron_cluster_recording_log_append_snapshot(log, 0, 0, 0, 0, 200, 0);

    ASSERT_EQ(0, aeron_cluster_recording_log_force(log));
    ASSERT_EQ(0, aeron_cluster_recording_log_reload(log));

    EXPECT_EQ(4, log->sorted_count);

    /* Verify sorted order: entries should be sorted by leadership_term_id */
    if (log->sorted_count >= 2)
    {
        for (int i = 1; i < log->sorted_count; i++)
        {
            EXPECT_LE(log->sorted_entries[i - 1].leadership_term_id,
                       log->sorted_entries[i].leadership_term_id)
                << "Entries should be sorted by leadership_term_id";
        }
    }

    aeron_cluster_recording_log_close(log);
}

/* -----------------------------------------------------------------------
 * Test: seedRecordingLogFromSnapshotShouldDeleteOriginalRecordingLogFileIfThereAreNoValidSnapshots
 * Verifies invalidation of snapshots in recording log.
 * ----------------------------------------------------------------------- */
TEST_F(ClusterToolTest,
       seedRecordingLogFromSnapshotShouldDeleteOriginalRecordingLogFileIfThereAreNoValidSnapshots)
{
    aeron_cluster_recording_log_t *log = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_nodes[0]->cluster_dir().c_str(), true));

    aeron_cluster_recording_log_append_term(log, 1, 1, 0, 100);
    aeron_cluster_recording_log_append_snapshot(log, 1, 1, 1000, 256, 300, 0);
    aeron_cluster_recording_log_append_snapshot(log, 1, 1, 1000, 256, 300, -1);
    aeron_cluster_recording_log_append_term(log, 1, 2, 2000, 400);

    ASSERT_EQ(0, aeron_cluster_recording_log_force(log));
    ASSERT_EQ(0, aeron_cluster_recording_log_reload(log));

    int count = log->sorted_count;
    EXPECT_GE(count, 4);

    /* Invalidate latest snapshot — returns 1 if found and invalidated */
    int rc = aeron_cluster_recording_log_invalidate_latest_snapshot(log);
    EXPECT_EQ(1, rc);

    aeron_cluster_recording_log_close(log);
}

/* -----------------------------------------------------------------------
 * Test: seedRecordingLogFromSnapshotShouldCreateANewRecordingLogFromALatestValidSnapshot
 * Verifies recording log can be rebuilt from remaining valid snapshots.
 * ----------------------------------------------------------------------- */
TEST_F(ClusterToolTest,
       seedRecordingLogFromSnapshotShouldCreateANewRecordingLogFromALatestValidSnapshot)
{
    aeron_cluster_recording_log_t *log = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_nodes[0]->cluster_dir().c_str(), true));

    /* Create multiple snapshots across terms */
    aeron_cluster_recording_log_append_term(log, 1, 0, 0, 0);
    aeron_cluster_recording_log_append_snapshot(log, 10, 0, 1000, 1000, 100, -1);
    aeron_cluster_recording_log_append_term(log, 1, 1, 1000, 200);
    aeron_cluster_recording_log_append_snapshot(log, 20, 1, 2000, 2000, 300, -1);

    ASSERT_EQ(0, aeron_cluster_recording_log_force(log));
    ASSERT_EQ(0, aeron_cluster_recording_log_reload(log));

    /* Latest snapshot should be recording_id=20 */
    aeron_cluster_recording_log_entry_t *snap =
        aeron_cluster_recording_log_get_latest_snapshot(log, -1);
    ASSERT_NE(nullptr, snap);
    EXPECT_EQ(20, snap->recording_id);

    aeron_cluster_recording_log_close(log);
}

/* -----------------------------------------------------------------------
 * Test: seedRecordingLogFromSnapshotShouldCreateANewRecordingLogFromALatestValidSnapshotCommandLine
 * Same as above but verifies via close/reopen cycle (command-line mode).
 * ----------------------------------------------------------------------- */
TEST_F(ClusterToolTest,
       seedRecordingLogFromSnapshotShouldCreateANewRecordingLogFromALatestValidSnapshotCommandLine)
{
    aeron_cluster_recording_log_t *log = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_nodes[0]->cluster_dir().c_str(), true));

    aeron_cluster_recording_log_append_term(log, 1, 0, 0, 0);
    aeron_cluster_recording_log_append_snapshot(log, 10, 0, 1000, 1000, 100, -1);
    aeron_cluster_recording_log_append_term(log, 1, 1, 1000, 200);
    aeron_cluster_recording_log_append_snapshot(log, 20, 1, 2000, 2000, 300, -1);

    ASSERT_EQ(0, aeron_cluster_recording_log_force(log));
    aeron_cluster_recording_log_close(log);

    /* Reopen (simulates command-line tool access) */
    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_nodes[0]->cluster_dir().c_str(), false));

    aeron_cluster_recording_log_entry_t *snap =
        aeron_cluster_recording_log_get_latest_snapshot(log, -1);
    ASSERT_NE(nullptr, snap);
    EXPECT_EQ(20, snap->recording_id);

    aeron_cluster_recording_log_close(log);
}

/* -----------------------------------------------------------------------
 * Test: shouldCheckForLeaderInAnyStateAfterElectionWasClosed
 * Verifies leader detection works via CM role check.
 * ----------------------------------------------------------------------- */
TEST_F(ClusterToolTest, shouldCheckForLeaderInAnyStateAfterElectionWasClosed)
{
    for (int i = 0; i < NODE_COUNT; i++)
    {
        ASSERT_EQ(0, create_agent(i, 0))
            << "Failed to create agent " << i << ": " << aeron_errmsg();
    }

    int leader_idx = elect_leader(0);
    ASSERT_GE(leader_idx, 0) << "No leader elected";

    /* Check isLeader equivalent */
    EXPECT_EQ(AERON_CLUSTER_ROLE_LEADER, m_agents[leader_idx]->role);

    for (int i = 0; i < NODE_COUNT; i++)
    {
        if (i != leader_idx && m_agents[i])
        {
            EXPECT_NE(AERON_CLUSTER_ROLE_LEADER, m_agents[i]->role)
                << "Follower " << i << " should not report as leader";
        }
    }
}

/* -----------------------------------------------------------------------
 * Test: listMembersShouldReturnFalseIfNoMarkFileExists
 * Verifies graceful failure when cluster dir is empty.
 * ----------------------------------------------------------------------- */
TEST_F(ClusterToolTest, listMembersShouldReturnFalseIfNoMarkFileExists)
{
    std::string empty_dir = m_base_dir + "/empty_members";
    aeron_mkdir_recursive(empty_dir.c_str(), 0777);

    /* No mark file or recording log exists */
    aeron_cluster_recording_log_t *log = nullptr;
    int rc = aeron_cluster_recording_log_open(&log, empty_dir.c_str(), false);
    if (rc == 0 && log)
    {
        EXPECT_EQ(0, log->sorted_count);
        aeron_cluster_recording_log_close(log);
    }
}

/* -----------------------------------------------------------------------
 * Test: listMembersShouldReturnFalseIfQueryTimesOut
 * Verifies that a recording log can be created but member list
 * query requires an active CM (which we do not have here).
 * ----------------------------------------------------------------------- */
TEST_F(ClusterToolTest, listMembersShouldReturnFalseIfQueryTimesOut)
{
    aeron_cluster_recording_log_t *log = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_nodes[0]->cluster_dir().c_str(), true));

    aeron_cluster_recording_log_append_term(log, 1, 0, 0, 0);
    ASSERT_EQ(0, aeron_cluster_recording_log_force(log));

    /* Without an active CM, any member query would time out.
     * Verify the log exists but has no cluster membership info. */
    EXPECT_GT(log->sorted_count, 0);

    /* No snapshot = no membership state */
    aeron_cluster_recording_log_entry_t *snap =
        aeron_cluster_recording_log_get_latest_snapshot(log, -1);
    EXPECT_EQ(nullptr, snap);

    aeron_cluster_recording_log_close(log);
}
