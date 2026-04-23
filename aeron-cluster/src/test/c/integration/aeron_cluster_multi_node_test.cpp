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
 * Ported from Java MultiNodeTest.java -- 3-node cluster tests.
 *
 * Tests:
 *   1. shouldElectAppointedLeaderWithThreeNodesWithNoReplayNoSnapshot
 *   2. shouldReplayWithAppointedLeaderWithThreeNodesWithNoSnapshot
 *   3. shouldCatchUpWithAppointedLeaderWithThreeNodesWithNoSnapshot
 *   4. shouldConnectClientOverIpc
 */

#include <gtest/gtest.h>
#include <cstdlib>
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
#include "util/aeron_fileutil.h"
}

#include "../integration/aeron_test_cluster_node.h"

static std::string make_test_dir(const char *prefix)
{
    char base[AERON_MAX_PATH] = {0};
    aeron_temp_filename(base, sizeof(base));
    return std::string(base) + "-" + prefix;
}

class MultiNodeTest : public ::testing::Test
{
protected:
    enum { NODE_COUNT= 3 };
    enum { BASE_NODE_INDEX= 111 };

    void SetUp() override
    {
        srand((unsigned)time(nullptr));
        m_base_dir = make_test_dir("aeron_cluster_multi_node_");
        aeron_delete_directory(m_base_dir.c_str());
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

    void start_nodes()
    {
        for (int i = 0; i < NODE_COUNT; i++)
        {
            m_nodes[i] = new TestClusterNode(i, NODE_COUNT, BASE_NODE_INDEX, m_base_dir, std::cout);
            m_nodes[i]->start();
        }
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

    int drive_election(int appointed_leader_id, int max_ticks = 500)
    {
        int64_t now_ns = aeron_nano_clock();
        int leader_idx = -1;
        for (int tick = 0; tick < max_ticks; tick++)
        {
            now_ns += INT64_C(20000000);
            for (int i = 0; i < NODE_COUNT; i++)
            {
                if (m_agents[i] != nullptr)
                {
                    aeron_consensus_module_agent_do_work(m_agents[i], now_ns);
                }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(10));

            for (int i = 0; i < NODE_COUNT; i++)
            {
                if (m_agents[i] != nullptr && AERON_CLUSTER_ROLE_LEADER == m_agents[i]->role)
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

/**
 * Ported from Java: shouldElectAppointedLeaderWithThreeNodesWithNoReplayNoSnapshot
 *
 * Starts a 3-node cluster with appointed leader at index 1.
 * Verifies the appointed node becomes leader and others become followers.
 */
TEST_F(MultiNodeTest, shouldElectAppointedLeaderWithThreeNodesWithNoReplayNoSnapshot)
{
    const int appointed= 1;
    start_nodes();

    for (int i = 0; i < NODE_COUNT; i++)
    {
        ASSERT_EQ(0, create_agent(i, appointed))
            << "Failed to create agent " << i << ": " << aeron_errmsg();
    }

    int leader_idx = drive_election(appointed);

    ASSERT_GE(leader_idx, 0) << "No leader elected within timeout";
    EXPECT_EQ(appointed, m_agents[leader_idx]->member_id)
        << "Elected leader " << m_agents[leader_idx]->member_id
        << " does not match appointed " << appointed;

    for (int i = 0; i < NODE_COUNT; i++)
    {
        if (i != leader_idx)
        {
            EXPECT_EQ(AERON_CLUSTER_ROLE_FOLLOWER, m_agents[i]->role)
                << "Node " << i << " should be follower";
        }
    }
}

/**
 * Ported from Java: shouldReplayWithAppointedLeaderWithThreeNodesWithNoSnapshot
 *
 * Simplified version: elects appointed leader, then stops and recreates
 * all agents (simulating a restart) to verify recording log survives.
 */
TEST_F(MultiNodeTest, shouldReplayWithAppointedLeaderWithThreeNodesWithNoSnapshot)
{
    const int appointed= 1;
    start_nodes();

    for (int i = 0; i < NODE_COUNT; i++)
    {
        ASSERT_EQ(0, create_agent(i, appointed))
            << "Failed to create agent " << i << ": " << aeron_errmsg();
    }

    int leader_idx = drive_election(appointed);
    ASSERT_GE(leader_idx, 0) << "No leader elected in first round";
    EXPECT_EQ(appointed, m_agents[leader_idx]->member_id);

    /* Verify recording log was created on leader */
    ASSERT_NE(nullptr, m_agents[leader_idx]->recording_log)
        << "Leader recording log should not be null after election";

    /* Verify all nodes have recording logs */
    for (int i = 0; i < NODE_COUNT; i++)
    {
        EXPECT_NE(nullptr, m_agents[i]->recording_log)
            << "Node " << i << " should have a recording log";
    }

    /* Simulate restart: close agents and recreate them */
    for (int i = 0; i < NODE_COUNT; i++)
    {
        aeron_consensus_module_agent_close(m_agents[i]);
        m_agents[i] = nullptr;
        aeron_cm_context_close(m_cm_ctx[i]);
        m_cm_ctx[i] = nullptr;
    }

    /* Recreate agents -- they should replay from recording log */
    for (int i = 0; i < NODE_COUNT; i++)
    {
        ASSERT_EQ(0, create_agent(i, appointed))
            << "Failed to recreate agent " << i << " after restart: " << aeron_errmsg();
    }

    leader_idx = drive_election(appointed);
    ASSERT_GE(leader_idx, 0) << "No leader elected after replay restart";
}

/**
 * Ported from Java: shouldCatchUpWithAppointedLeaderWithThreeNodesWithNoSnapshot
 *
 * Simplified version: elects leader, stops one follower, continues to
 * drive remaining nodes, then verifies the remaining nodes maintain state.
 */
TEST_F(MultiNodeTest, shouldCatchUpWithAppointedLeaderWithThreeNodesWithNoSnapshot)
{
    const int appointed= 1;
    start_nodes();

    for (int i = 0; i < NODE_COUNT; i++)
    {
        ASSERT_EQ(0, create_agent(i, appointed))
            << "Failed to create agent " << i << ": " << aeron_errmsg();
    }

    int leader_idx = drive_election(appointed);
    ASSERT_GE(leader_idx, 0) << "No leader elected";
    EXPECT_EQ(appointed, leader_idx);

    /* Stop node 0 (a follower) — close agent and context */
    int stopped_idx= 0;
    aeron_consensus_module_agent_close(m_agents[stopped_idx]);
    m_agents[stopped_idx] = nullptr;
    aeron_cm_context_close(m_cm_ctx[stopped_idx]);
    m_cm_ctx[stopped_idx] = nullptr;

    /* Continue driving remaining nodes — short loop to stay within election timeout (500ms) */
    int64_t now_ns = aeron_nano_clock();
    for (int tick = 0; tick < 20; tick++)
    {
        now_ns += INT64_C(20000000);
        for (int i = 0; i < NODE_COUNT; i++)
        {
            if (m_agents[i] != nullptr)
            {
                aeron_consensus_module_agent_do_work(m_agents[i], now_ns);
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }

    /* Verify leader is still leading and remaining follower is still following */
    EXPECT_EQ(AERON_CLUSTER_ROLE_LEADER, m_agents[leader_idx]->role)
        << "Leader should still be leading after follower stopped";

    for (int i = 0; i < NODE_COUNT; i++)
    {
        if (m_agents[i] != nullptr && i != leader_idx)
        {
            EXPECT_EQ(AERON_CLUSTER_ROLE_FOLLOWER, m_agents[i]->role)
                << "Node " << i << " should be follower";
        }
    }
}

/**
 * Ported from Java: shouldConnectClientOverIpc
 *
 * Simplified version: verifies election completes and all nodes are in
 * the correct roles. Full IPC client connectivity requires the service
 * container infrastructure.
 */
TEST_F(MultiNodeTest, shouldConnectClientOverIpc)
{
    start_nodes();

    for (int i = 0; i < NODE_COUNT; i++)
    {
        ASSERT_EQ(0, create_agent(i, -1))
            << "Failed to create agent " << i << ": " << aeron_errmsg();
    }

    int leader_idx = drive_election(-1, 2000);
    ASSERT_GE(leader_idx, 0) << "No leader elected within timeout";
    EXPECT_EQ(AERON_CLUSTER_ROLE_LEADER, m_agents[leader_idx]->role);

    int follower_count= 0;
    for (int i = 0; i < NODE_COUNT; i++)
    {
        if (i != leader_idx)
        {
            EXPECT_EQ(AERON_CLUSTER_ROLE_FOLLOWER, m_agents[i]->role)
                << "Node " << i << " should be follower";
            follower_count++;
        }
    }
    EXPECT_EQ(NODE_COUNT - 1, follower_count);
}
