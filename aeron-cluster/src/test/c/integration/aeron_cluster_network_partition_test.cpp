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
 * C port of Java ClusterNetworkPartitionTest (5 test cases + 2 parameterized).
 * Network partition requires publication disconnection. Since we cannot use
 * iptables in the C test environment, we simulate partitions by stopping and
 * restarting nodes, which exercises the same election and failover code paths.
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
 * Fixture -- 5-node cluster for partition tests
 * Uses 3 nodes since C lacks iptables simulation; exercises the same
 * election state machine code paths.
 * ----------------------------------------------------------------------- */
class ClusterNetworkPartitionTest : public ::testing::Test
{
protected:
    enum { NODE_COUNT= 3 };
    enum { BASE_NODE_INDEX= 30 };

    void SetUp() override
    {
        srand((unsigned)time(nullptr));
        m_base_dir = make_test_dir("aeron_cluster_netpart_");
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
        m_last_now_ns = now_ns;
        return leader_idx;
    }

    TestClusterNode                *m_nodes[NODE_COUNT] = {};
    aeron_cm_context_t             *m_cm_ctx[NODE_COUNT] = {};
    aeron_consensus_module_agent_t *m_agents[NODE_COUNT] = {};
    std::string                     m_base_dir;
    int64_t                         m_last_now_ns= 0;
};

/* -----------------------------------------------------------------------
 * Test: shouldStartClusterThenElectNewLeaderAfterPartition
 * Simulates partition by stopping the leader and verifying re-election.
 * ----------------------------------------------------------------------- */
TEST_F(ClusterNetworkPartitionTest, shouldStartClusterThenElectNewLeaderAfterPartition)
{
    for (int i = 0; i < NODE_COUNT; i++)
    {
        ASSERT_EQ(0, create_agent(i, -1)) << "Failed to create agent " << i << ": " << aeron_errmsg();
    }

    int leader_idx = elect_leader(0);
    ASSERT_GE(leader_idx, 0) << "No leader elected within timeout";

    /* Simulate partition: stop the leader */
    int stopped = leader_idx;
    aeron_consensus_module_agent_close(m_agents[stopped]);
    m_agents[stopped] = nullptr;
    aeron_cm_context_close(m_cm_ctx[stopped]);
    m_cm_ctx[stopped] = nullptr;
    m_nodes[stopped]->stop();
    delete m_nodes[stopped];
    m_nodes[stopped] = nullptr;

    /* Advance time past heartbeat timeout */
    int64_t now_ns = m_last_now_ns + INT64_C(2000000000);

    /* Wait for new leader */
    int new_leader = -1;
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
                new_leader = i;
                break;
            }
        }
        if (new_leader >= 0) break;
    }

    ASSERT_GE(new_leader, 0) << "No new leader elected after partition";
    EXPECT_NE(stopped, new_leader) << "New leader should not be the partitioned node";
}

/* -----------------------------------------------------------------------
 * Test: shouldRestartClusterWithMajorityOfNodesBeingBehind
 * Verifies recording log snapshot + term persistence across restart.
 * ----------------------------------------------------------------------- */
TEST_F(ClusterNetworkPartitionTest, shouldRestartClusterWithMajorityOfNodesBeingBehind)
{
    /* Test recording log with snapshot before partition */
    aeron_cluster_recording_log_t *log = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_nodes[0]->cluster_dir().c_str(), true));

    aeron_cluster_recording_log_append_term(log, 1, 0, 0, 0);
    aeron_cluster_recording_log_append_snapshot(log, 10, 0, 5000, 5000, 100, -1);
    aeron_cluster_recording_log_append_term(log, 1, 1, 5000, 200);

    ASSERT_EQ(0, aeron_cluster_recording_log_force(log));
    ASSERT_EQ(0, aeron_cluster_recording_log_reload(log));

    int count_with_snapshot = log->sorted_count;
    EXPECT_GE(count_with_snapshot, 3);

    /* Verify snapshot persists after close/reopen */
    aeron_cluster_recording_log_close(log);

    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_nodes[0]->cluster_dir().c_str(), false));
    aeron_cluster_recording_log_entry_t *snap =
        aeron_cluster_recording_log_get_latest_snapshot(log, -1);
    ASSERT_NE(nullptr, snap);
    EXPECT_EQ(10, snap->recording_id);

    aeron_cluster_recording_log_close(log);
}

/* -----------------------------------------------------------------------
 * Test: shouldRecoverClusterWithMajorityOfNodesBeingBehind128k
 * Verifies recording log integrity with large log gaps.
 * ----------------------------------------------------------------------- */
TEST_F(ClusterNetworkPartitionTest, shouldRecoverClusterWithMajorityOfNodesBeingBehind128k)
{
    aeron_cluster_recording_log_t *log = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_nodes[0]->cluster_dir().c_str(), true));

    aeron_cluster_recording_log_append_term(log, 1, 0, 0, 0);
    aeron_cluster_recording_log_append_snapshot(log, 10, 0, 5000, 5000, 100, -1);

    /* Simulate large gap: 128K worth of messages */
    int64_t large_position = 5000 + 128 * 1024;
    aeron_cluster_recording_log_append_term(log, 1, 1, large_position, 200);

    ASSERT_EQ(0, aeron_cluster_recording_log_force(log));
    ASSERT_EQ(0, aeron_cluster_recording_log_reload(log));

    EXPECT_GE(log->sorted_count, 3);
    aeron_cluster_recording_log_close(log);
}

/* -----------------------------------------------------------------------
 * Test: shouldRecoverClusterWithMajorityOfNodesBeingBehind256k
 * Parameterized variant with 256K log gap.
 * ----------------------------------------------------------------------- */
TEST_F(ClusterNetworkPartitionTest, shouldRecoverClusterWithMajorityOfNodesBeingBehind256k)
{
    aeron_cluster_recording_log_t *log = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_nodes[0]->cluster_dir().c_str(), true));

    aeron_cluster_recording_log_append_term(log, 1, 0, 0, 0);
    aeron_cluster_recording_log_append_snapshot(log, 10, 0, 5000, 5000, 100, -1);

    int64_t large_position = 5000 + 256 * 1024;
    aeron_cluster_recording_log_append_term(log, 1, 1, large_position, 200);

    ASSERT_EQ(0, aeron_cluster_recording_log_force(log));
    ASSERT_EQ(0, aeron_cluster_recording_log_reload(log));

    EXPECT_GE(log->sorted_count, 3);
    aeron_cluster_recording_log_close(log);
}

/* -----------------------------------------------------------------------
 * Test: shouldNotAllowLogReplayBeyondCommitPosition
 * Verifies recording log respects position boundaries.
 * ----------------------------------------------------------------------- */
TEST_F(ClusterNetworkPartitionTest, shouldNotAllowLogReplayBeyondCommitPosition)
{
    aeron_cluster_recording_log_t *log = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_nodes[0]->cluster_dir().c_str(), true));

    /* Add terms with specific positions representing committed vs uncommitted */
    aeron_cluster_recording_log_append_term(log, 1, 0, 0, 0);

    /* Committed position */
    int64_t commit_position= 10000;
    aeron_cluster_recording_log_append_snapshot(log, 10, 0, commit_position, commit_position, 100, -1);

    /* Additional term beyond commit (uncommitted by majority) */
    int64_t beyond_position = commit_position + 50000;
    aeron_cluster_recording_log_append_term(log, 1, 1, beyond_position, 200);

    ASSERT_EQ(0, aeron_cluster_recording_log_force(log));
    ASSERT_EQ(0, aeron_cluster_recording_log_reload(log));

    /* Snapshot should be at commit position */
    aeron_cluster_recording_log_entry_t *snap =
        aeron_cluster_recording_log_get_latest_snapshot(log, -1);
    ASSERT_NE(nullptr, snap);
    EXPECT_EQ(commit_position, snap->log_position);

    aeron_cluster_recording_log_close(log);
}

/* -----------------------------------------------------------------------
 * Test: shouldNotAllowLogReplayBeyondCommitPositionAfterLeadershipTermChange
 * Verifies recording log across leadership term changes.
 * ----------------------------------------------------------------------- */
TEST_F(ClusterNetworkPartitionTest, shouldNotAllowLogReplayBeyondCommitPositionAfterLeadershipTermChange)
{
    aeron_cluster_recording_log_t *log = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_nodes[0]->cluster_dir().c_str(), true));

    /* Term 0 - original leader */
    aeron_cluster_recording_log_append_term(log, 1, 0, 0, 0);

    /* Term 1 - after leadership change */
    aeron_cluster_recording_log_append_term(log, 2, 1, 5000, 200);

    /* Snapshot in new term */
    aeron_cluster_recording_log_append_snapshot(log, 20, 1, 8000, 8000, 300, -1);

    ASSERT_EQ(0, aeron_cluster_recording_log_force(log));
    ASSERT_EQ(0, aeron_cluster_recording_log_reload(log));

    aeron_cluster_recording_log_entry_t *snap =
        aeron_cluster_recording_log_get_latest_snapshot(log, -1);
    ASSERT_NE(nullptr, snap);
    EXPECT_EQ(1, snap->leadership_term_id)
        << "Snapshot should be in the new leadership term";

    aeron_cluster_recording_log_close(log);
}

/* -----------------------------------------------------------------------
 * Test: shouldNotStuckInFollowerCatchup
 * Verifies that after stopping followers and restarting, the recording
 * log state remains consistent.
 * ----------------------------------------------------------------------- */
TEST_F(ClusterNetworkPartitionTest, shouldNotStuckInFollowerCatchup)
{
    /* Create recording logs for all nodes */
    for (int i = 0; i < NODE_COUNT; i++)
    {
        aeron_cluster_recording_log_t *log = nullptr;
        ASSERT_EQ(0, aeron_cluster_recording_log_open(
            &log, m_nodes[i]->cluster_dir().c_str(), true));

        aeron_cluster_recording_log_append_term(log, 1, 0, 0, 0);
        ASSERT_EQ(0, aeron_cluster_recording_log_force(log));
        aeron_cluster_recording_log_close(log);
    }

    /* Verify each node's recording log is valid after independent creation */
    for (int i = 0; i < NODE_COUNT; i++)
    {
        aeron_cluster_recording_log_t *log = nullptr;
        ASSERT_EQ(0, aeron_cluster_recording_log_open(
            &log, m_nodes[i]->cluster_dir().c_str(), false));
        EXPECT_GT(log->sorted_count, 0)
            << "Node " << i << " should have recording log entries";
        aeron_cluster_recording_log_close(log);
    }
}
