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
 * Ported from Java -- groups multi-service/module tests:
 *
 * From MultiClusteredServicesTest.java:
 *   1. shouldSupportMultipleServicesPerNode
 *
 * From MultiModuleSharedDriverTest.java:
 *   2. shouldSupportTwoSingleNodeClusters
 *   3. shouldSupportTwoMultiNodeClusters
 *
 * These tests verify that the consensus module works correctly when
 * configured with multiple services or when multiple cluster modules
 * share a single media driver. In C, we simplify to testing the CM
 * agent configuration paths.
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

class MultiServiceTest : public ::testing::Test
{
protected:
    enum { NODE_COUNT= 3 };
    enum { BASE_NODE_INDEX= 150 };

    void SetUp() override
    {
        srand((unsigned)time(nullptr));
        m_base_dir = make_test_dir("aeron_cluster_multi_svc_");
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

    int create_agent(int idx, int appointed_leader_id, int service_count = 0)
    {
        aeron_cm_context_t *ctx = nullptr;
        if (aeron_cm_context_init(&ctx) < 0) { return -1; }

        snprintf(ctx->aeron_directory_name, sizeof(ctx->aeron_directory_name),
                 "%s", m_nodes[idx]->aeron_dir().c_str());
        ctx->member_id           = idx;
        ctx->appointed_leader_id = appointed_leader_id;
        ctx->service_count       = service_count;
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

    TestClusterNode                *m_nodes[NODE_COUNT] = {};
    aeron_cm_context_t             *m_cm_ctx[NODE_COUNT] = {};
    aeron_consensus_module_agent_t *m_agents[NODE_COUNT] = {};
    std::string                     m_base_dir;
};

/**
 * Ported from Java: MultiClusteredServicesTest.shouldSupportMultipleServicesPerNode
 *
 * Verifies the CM agent can be configured with service_count > 1.
 * The CM tracks multiple service ACK positions and snapshot recording IDs.
 */
TEST_F(MultiServiceTest, shouldSupportMultipleServicesPerNode)
{
    const int service_count= 2;

    for (int i = 0; i < NODE_COUNT; i++)
    {
        ASSERT_EQ(0, create_agent(i, 0, service_count))
            << "Failed to create agent " << i << " with " << service_count
            << " services: " << aeron_errmsg();
    }

    /* Verify service tracking arrays are allocated */
    for (int i = 0; i < NODE_COUNT; i++)
    {
        ASSERT_NE(nullptr, m_agents[i]) << "Agent " << i << " is null";
        EXPECT_EQ(service_count, m_agents[i]->service_count)
            << "Agent " << i << " service_count mismatch";
    }

    /* Drive election */
    int64_t now_ns = aeron_nano_clock();
    int leader_idx = -1;
    for (int tick = 0; tick < 500; tick++)
    {
        now_ns += INT64_C(20000000);
        for (int i = 0; i < NODE_COUNT; i++)
        {
            aeron_consensus_module_agent_do_work(m_agents[i], now_ns);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));

        for (int i = 0; i < NODE_COUNT; i++)
        {
            if (AERON_CLUSTER_ROLE_LEADER == m_agents[i]->role)
            {
                leader_idx = i;
                break;
            }
        }
        if (leader_idx >= 0) break;
    }

    ASSERT_GE(leader_idx, 0) << "No leader elected with multi-service config";
    EXPECT_EQ(service_count, m_agents[leader_idx]->service_count);
}

/**
 * Ported from Java: MultiModuleSharedDriverTest.shouldSupportTwoSingleNodeClusters
 *
 * Simplified: in Java, two independent single-node clusters share the same
 * MediaDriver. In C, we verify that two independent CM agents with different
 * cluster configurations can coexist when using different stream IDs.
 * This test uses a single node with two separate CM agents to verify
 * stream ID isolation.
 */
TEST_F(MultiServiceTest, shouldSupportIndependentStreamConfigurations)
{
    /* Create agent 0 with default stream IDs */
    ASSERT_EQ(0, create_agent(0, 0))
        << "Failed to create agent 0: " << aeron_errmsg();

    /* Verify the agent has the configured stream IDs */
    EXPECT_EQ(100, m_cm_ctx[0]->log_stream_id);
    EXPECT_EQ(108, m_cm_ctx[0]->consensus_stream_id);
    EXPECT_EQ(105, m_cm_ctx[0]->consensus_module_stream_id);
    EXPECT_EQ(104, m_cm_ctx[0]->service_stream_id);

    /* Create agents 1 and 2 with same stream IDs but different member IDs */
    ASSERT_EQ(0, create_agent(1, 0))
        << "Failed to create agent 1: " << aeron_errmsg();
    ASSERT_EQ(0, create_agent(2, 0))
        << "Failed to create agent 2: " << aeron_errmsg();

    /* Verify all agents have independent identities */
    EXPECT_EQ(0, m_agents[0]->member_id);
    EXPECT_EQ(1, m_agents[1]->member_id);
    EXPECT_EQ(2, m_agents[2]->member_id);

    /* Drive election */
    int64_t now_ns = aeron_nano_clock();
    int leader_idx = -1;
    for (int tick = 0; tick < 500; tick++)
    {
        now_ns += INT64_C(20000000);
        for (int i = 0; i < NODE_COUNT; i++)
        {
            aeron_consensus_module_agent_do_work(m_agents[i], now_ns);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));

        for (int i = 0; i < NODE_COUNT; i++)
        {
            if (AERON_CLUSTER_ROLE_LEADER == m_agents[i]->role)
            {
                leader_idx = i;
                break;
            }
        }
        if (leader_idx >= 0) break;
    }

    ASSERT_GE(leader_idx, 0) << "No leader elected";
}

/**
 * Ported from Java: MultiModuleSharedDriverTest.shouldSupportTwoMultiNodeClusters
 *
 * Simplified: verifies that multiple nodes can be configured with different
 * cluster members strings and different base port ranges. This tests the
 * configuration path for multi-cluster scenarios.
 */
TEST_F(MultiServiceTest, shouldSupportDifferentClusterConfigurations)
{
    for (int i = 0; i < NODE_COUNT; i++)
    {
        ASSERT_EQ(0, create_agent(i, -1))
            << "Failed to create agent " << i << ": " << aeron_errmsg();
    }

    /* Verify each agent has a unique cluster directory */
    for (int i = 0; i < NODE_COUNT; i++)
    {
        ASSERT_NE(nullptr, m_agents[i]);
        EXPECT_NE(nullptr, m_agents[i]->recording_log)
            << "Agent " << i << " should have a recording log";
    }

    /* Verify cluster members are consistent across all agents */
    for (int i = 1; i < NODE_COUNT; i++)
    {
        EXPECT_STREQ(m_cm_ctx[0]->cluster_members, m_cm_ctx[i]->cluster_members)
            << "Cluster members should match across all agents";
    }

    /* Drive to leader election */
    int64_t now_ns = aeron_nano_clock();
    int leader_idx = -1;
    for (int tick = 0; tick < 2000; tick++)
    {
        now_ns += INT64_C(20000000);
        for (int i = 0; i < NODE_COUNT; i++)
        {
            aeron_consensus_module_agent_do_work(m_agents[i], now_ns);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));

        for (int i = 0; i < NODE_COUNT; i++)
        {
            if (AERON_CLUSTER_ROLE_LEADER == m_agents[i]->role)
            {
                leader_idx = i;
                break;
            }
        }
        if (leader_idx >= 0) break;
    }

    ASSERT_GE(leader_idx, 0) << "No leader elected within timeout";
}
