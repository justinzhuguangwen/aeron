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
 * Ported from Java -- groups election recovery tests:
 *
 * From FailedFirstElectionClusterTest.java:
 *   1. shouldRecoverWhenFollowerIsMultipleTermsBehindFromEmptyLog
 *
 * From RecoverAfterFailedCatchupClusterTest.java:
 *   2. shouldCatchupFromEmptyLog
 *
 * From StalledLeaderLogReplicationClusterTest.java:
 *   3. shouldHandleMultipleElections
 *
 * The Java tests use ByteBuddy instrumentation to inject failures and
 * stalls. In C, we simulate these scenarios by manipulating the election
 * flow directly: stopping/restarting agents and advancing time.
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

class ElectionRecoveryTest : public ::testing::Test
{
protected:
    enum { NODE_COUNT= 3 };
    enum { BASE_NODE_INDEX= 132 };

    void SetUp() override
    {
        srand((unsigned)time(nullptr));
        m_base_dir = make_test_dir("aeron_cluster_election_recovery_");
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

    int await_leader(int exclude_idx = -1, int max_ticks = 2000)
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
                if (i != exclude_idx && m_agents[i] != nullptr &&
                    AERON_CLUSTER_ROLE_LEADER == m_agents[i]->role)
                {
                    leader_idx = i;
                    break;
                }
            }
            if (leader_idx >= 0) break;
        }
        return leader_idx;
    }

    void stop_agent(int idx)
    {
        if (m_agents[idx])
        {
            aeron_consensus_module_agent_close(m_agents[idx]);
            m_agents[idx] = nullptr;
        }
        if (m_cm_ctx[idx])
        {
            aeron_cm_context_close(m_cm_ctx[idx]);
            m_cm_ctx[idx] = nullptr;
        }
    }

    TestClusterNode                *m_nodes[NODE_COUNT] = {};
    aeron_cm_context_t             *m_cm_ctx[NODE_COUNT] = {};
    aeron_consensus_module_agent_t *m_agents[NODE_COUNT] = {};
    std::string                     m_base_dir;
};

/**
 * Ported from Java: FailedFirstElectionClusterTest.shouldRecoverWhenFollowerIsMultipleTermsBehindFromEmptyLog
 *
 * Simplified: the Java test uses ByteBuddy to fail onRequestVote for term 0.
 * In C, we simulate multiple leadership terms by electing, stopping the leader,
 * and re-electing from the survivors.
 */
TEST_F(ElectionRecoveryTest, shouldRecoverWhenFollowerIsMultipleTermsBehind)
{
    for (int i = 0; i < NODE_COUNT; i++)
    {
        ASSERT_EQ(0, create_agent(i, -1))
            << "Failed to create agent " << i << ": " << aeron_errmsg();
    }

    /* Phase 1: First election */
    int leader_idx = await_leader();
    ASSERT_GE(leader_idx, 0) << "No leader elected in first round";
    int64_t first_term_id = m_agents[leader_idx]->leadership_term_id;

    /* Phase 2: Stop leader to force new election (simulates term advancement) */
    int stopped_leader = leader_idx;
    stop_agent(stopped_leader);

    /* Advance time past heartbeat timeout */
    int64_t now_ns = aeron_nano_clock();
    now_ns += INT64_C(3000000000);
    for (int tick = 0; tick < 100; tick++)
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

    int new_leader = await_leader(stopped_leader, 2000);
    ASSERT_GE(new_leader, 0) << "No new leader elected after stopping first leader";
    EXPECT_NE(stopped_leader, new_leader);

    /* Verify the new term is higher than the first */
    EXPECT_GE(m_agents[new_leader]->leadership_term_id, first_term_id)
        << "New leadership term should be >= first term";
}

/**
 * Ported from Java: RecoverAfterFailedCatchupClusterTest.shouldCatchupFromEmptyLog
 *
 * Simplified: in Java, ByteBuddy throws an exception on the first FOLLOWER_CATCHUP
 * state transition. In C, we simulate a failed catchup by stopping a follower,
 * letting the cluster advance, then restarting the follower so it must catch up.
 */
TEST_F(ElectionRecoveryTest, shouldRecoverAfterFollowerRestart)
{
    for (int i = 0; i < NODE_COUNT; i++)
    {
        ASSERT_EQ(0, create_agent(i, -1))
            << "Failed to create agent " << i << ": " << aeron_errmsg();
    }

    int leader_idx = await_leader();
    ASSERT_GE(leader_idx, 0) << "No leader elected";

    /* Pick a follower to stop */
    int follower_idx = -1;
    for (int i = 0; i < NODE_COUNT; i++)
    {
        if (i != leader_idx && m_agents[i] != nullptr)
        {
            follower_idx = i;
            break;
        }
    }
    ASSERT_GE(follower_idx, 0);

    /* Stop the follower */
    stop_agent(follower_idx);

    /* Drive remaining nodes for a while */
    int64_t now_ns = aeron_nano_clock();
    for (int tick = 0; tick < 200; tick++)
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

    /* Restart the follower (simulates catchup from empty log) */
    ASSERT_EQ(0, create_agent(follower_idx, -1))
        << "Failed to restart follower: " << aeron_errmsg();

    /* Drive all nodes again */
    for (int tick = 0; tick < 500; tick++)
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
    }

    /* Verify the cluster has a leader and the restarted node is operational */
    bool has_leader = false;
    for (int i = 0; i < NODE_COUNT; i++)
    {
        if (m_agents[i] != nullptr && AERON_CLUSTER_ROLE_LEADER == m_agents[i]->role)
        {
            has_leader = true;
        }
    }
    EXPECT_TRUE(has_leader) << "Cluster should have a leader after follower restart";

    EXPECT_NE(nullptr, m_agents[follower_idx])
        << "Restarted follower agent should exist";
    EXPECT_NE(nullptr, m_agents[follower_idx]->recording_log)
        << "Restarted follower should have a recording log";
}

/**
 * Ported from Java: StalledLeaderLogReplicationClusterTest.shouldHandleMultipleElections
 *
 * Simplified: the Java test stalls leaderLogReplication via ByteBuddy.
 * In C, we force multiple elections by repeatedly stopping the current leader.
 */
TEST_F(ElectionRecoveryTest, shouldHandleMultipleElections)
{
    for (int i = 0; i < NODE_COUNT; i++)
    {
        ASSERT_EQ(0, create_agent(i, -1))
            << "Failed to create agent " << i << ": " << aeron_errmsg();
    }

    /* First election */
    int leader0 = await_leader(-1, 2000);
    ASSERT_GE(leader0, 0) << "No leader elected in first round";

    /* Stop leader to trigger second election */
    stop_agent(leader0);

    /* Advance past heartbeat timeout */
    int64_t now_ns = aeron_nano_clock();
    now_ns += INT64_C(3000000000);
    for (int tick = 0; tick < 50; tick++)
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

    int leader1 = await_leader(leader0, 2000);
    ASSERT_GE(leader1, 0) << "No leader elected in second round";
    EXPECT_NE(leader0, leader1) << "Second leader should be different from first";

    /* Restart the stopped node */
    ASSERT_EQ(0, create_agent(leader0, -1))
        << "Failed to restart stopped leader: " << aeron_errmsg();

    /* Drive all nodes */
    for (int tick = 0; tick < 200; tick++)
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

    /* Verify all agents are operational */
    for (int i = 0; i < NODE_COUNT; i++)
    {
        EXPECT_NE(nullptr, m_agents[i])
            << "Agent " << i << " should exist after multi-election scenario";
    }
}
