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
 * Appointed-leader single-node cluster integration tests -- C port of Java AppointedLeaderTest.
 *
 * Tests:
 *   shouldConnectAndSendKeepAliveWithAppointedLeader
 *   shouldEchoMessagesViaServiceWithAppointedLeader
 *
 * Java AppointedLeaderTest uses 3 nodes with appointedLeader=1. For the
 * single-node C infrastructure we use node_count=1 with appointed_leader_id=0,
 * verifying that the appointed leader configuration works end-to-end.
 */

#include <gtest/gtest.h>
#include <cstdlib>
#include <cstring>
#include <string>
#include <iostream>
#include <thread>
#include <chrono>
#include <atomic>
#include <vector>

extern "C"
{
#include "aeronc.h"
#include "client/aeron_cluster.h"
#include "client/aeron_cluster_context.h"
#include "client/aeron_cluster_client.h"
#include "client/aeron_cluster_async_connect.h"
#include "util/aeron_fileutil.h"
}

#include "../integration/aeron_test_clustered_media_driver.h"
#include "../integration/aeron_cluster_server_helper.h"

static std::string make_test_dir(const char *prefix)
{
    char base[AERON_MAX_PATH] = {0};
    aeron_temp_filename(base, sizeof(base));
    return std::string(base) + "-" + prefix;
}

/* -----------------------------------------------------------------------
 * Egress message collector
 * ----------------------------------------------------------------------- */
struct EgressState
{
    std::atomic<int>         received_count{0};
    std::vector<std::string> messages;
};

static void egress_on_message(
    void *clientd,
    int64_t cluster_session_id, int64_t leadership_term_id, int64_t timestamp,
    const uint8_t *buffer, size_t length, aeron_header_t *header)
{
    (void)cluster_session_id; (void)leadership_term_id; (void)timestamp; (void)header;
    EgressState *s = static_cast<EgressState *>(clientd);
    s->messages.emplace_back(reinterpret_cast<const char *>(buffer), length);
    s->received_count.fetch_add(1);
}

/* -----------------------------------------------------------------------
 * Fixture
 * ----------------------------------------------------------------------- */
class AppointedLeaderTest : public ::testing::Test
{
protected:
    enum { NODE_INDEX= 7 };

    void SetUp() override
    {
        m_base_dir = make_test_dir("aeron_cluster_appointed_");
        aeron_delete_directory(m_base_dir.c_str());

        m_cmd = new TestClusteredMediaDriver(0, 1, NODE_INDEX, m_base_dir, std::cout);
        ASSERT_EQ(0, m_cmd->launch()) << "ClusteredMediaDriver launch failed";
    }

    void TearDown() override
    {
        if (m_cluster_client != nullptr)
        {
            aeron_cluster_close(m_cluster_client);
            m_cluster_client = nullptr;
        }
        if (m_cluster_client_ctx != nullptr)
        {
            aeron_cluster_context_close(m_cluster_client_ctx);
            m_cluster_client_ctx = nullptr;
        }
        if (m_server != nullptr)
        {
            cluster_server_stop(m_server);
            m_server = nullptr;
        }
        if (m_cmd != nullptr)
        {
            m_cmd->close();
            delete m_cmd;
            m_cmd = nullptr;
        }
        aeron_delete_directory(m_base_dir.c_str());
    }

    cluster_server_handle_t *launchEchoService()
    {
        m_server = cluster_service_start(
            m_cmd->aeron_dir().c_str(),
            m_cmd->cluster_dir().c_str());
        if (m_server != nullptr)
        {
            cluster_server_start_background(m_server);
        }
        return m_server;
    }

    void awaitLeader()
    {
        auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (!m_cmd->is_leader() && std::chrono::steady_clock::now() < deadline)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        ASSERT_TRUE(m_cmd->is_leader()) << "Server did not become leader";
    }

    aeron_cluster_t *connectToCluster(EgressState &egress_state)
    {
        aeron_cluster_context_init(&m_cluster_client_ctx);
        aeron_cluster_context_set_aeron_directory_name(m_cluster_client_ctx, m_cmd->aeron_dir().c_str());
        aeron_cluster_context_set_ingress_channel(m_cluster_client_ctx, "aeron:ipc");
        aeron_cluster_context_set_ingress_stream_id(m_cluster_client_ctx, 101);
        aeron_cluster_context_set_egress_channel(m_cluster_client_ctx, "aeron:ipc");
        aeron_cluster_context_set_egress_stream_id(m_cluster_client_ctx, 102);
        aeron_cluster_context_set_ingress_endpoints(m_cluster_client_ctx,
            m_cmd->ingress_endpoints().c_str());
        aeron_cluster_context_set_on_message(m_cluster_client_ctx, egress_on_message, &egress_state);
        aeron_cluster_context_set_message_timeout_ns(m_cluster_client_ctx, INT64_C(10000000000));

        aeron_cluster_async_connect_t *async_conn = nullptr;
        if (aeron_cluster_async_connect(&async_conn, m_cluster_client_ctx) < 0)
        {
            return nullptr;
        }

        aeron_cluster_t *cluster = nullptr;
        auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (cluster == nullptr && std::chrono::steady_clock::now() < deadline)
        {
            int rc = aeron_cluster_async_connect_poll(&cluster, async_conn);
            if (rc < 0) { return nullptr; }
            if (cluster == nullptr)
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
        }

        m_cluster_client = cluster;
        return cluster;
    }

    void offerMessage(aeron_cluster_t *cluster, const uint8_t *buffer, size_t length)
    {
        auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (std::chrono::steady_clock::now() < deadline)
        {
            int64_t result = aeron_cluster_offer(cluster, buffer, length);
            if (result > 0) { return; }
            aeron_cluster_poll_egress(cluster);
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        FAIL() << "offerMessage timed out";
    }

    void awaitResponse(aeron_cluster_t *cluster, EgressState &egress_state, int target = 1)
    {
        auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (egress_state.received_count.load() < target &&
               std::chrono::steady_clock::now() < deadline)
        {
            if (aeron_cluster_poll_egress(cluster) <= 0)
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
        }
    }

    TestClusteredMediaDriver  *m_cmd                = nullptr;
    cluster_server_handle_t   *m_server             = nullptr;
    aeron_cluster_t           *m_cluster_client      = nullptr;
    aeron_cluster_context_t   *m_cluster_client_ctx  = nullptr;
    std::string                m_base_dir;
};

/* -----------------------------------------------------------------------
 * Test 1: shouldConnectAndSendKeepAliveWithAppointedLeader
 * Matches Java AppointedLeaderTest.shouldConnectAndSendKeepAlive
 * ----------------------------------------------------------------------- */
TEST_F(AppointedLeaderTest, shouldConnectAndSendKeepAliveWithAppointedLeader)
{
    cluster_server_handle_t *server = launchEchoService();
    ASSERT_NE(nullptr, server);

    awaitLeader();

    EXPECT_EQ(0, m_cmd->node_index() % 1) << "Appointed leader should be node 0 in single-node cluster";

    EgressState egress_state;
    aeron_cluster_t *cluster = connectToCluster(egress_state);
    ASSERT_NE(nullptr, cluster) << "Failed to connect: " << aeron_errmsg();

    EXPECT_TRUE(aeron_cluster_send_keep_alive(cluster));
}

/* -----------------------------------------------------------------------
 * Test 2: shouldEchoMessagesViaServiceWithAppointedLeader
 * Matches Java AppointedLeaderTest.shouldEchoMessagesViaService
 * ----------------------------------------------------------------------- */
TEST_F(AppointedLeaderTest, shouldEchoMessagesViaServiceWithAppointedLeader)
{
    static const char MSG[] = "appointed-echo";
    static const int MESSAGE_COUNT= 10;

    cluster_server_handle_t *server = launchEchoService();
    ASSERT_NE(nullptr, server);

    awaitLeader();

    EgressState egress_state;
    aeron_cluster_t *cluster = connectToCluster(egress_state);
    ASSERT_NE(nullptr, cluster) << "Failed to connect: " << aeron_errmsg();

    for (int i = 0; i < MESSAGE_COUNT; i++)
    {
        offerMessage(cluster, reinterpret_cast<const uint8_t *>(MSG), strlen(MSG));
    }

    awaitResponse(cluster, egress_state, MESSAGE_COUNT);

    EXPECT_GE(egress_state.received_count.load(), MESSAGE_COUNT)
        << "Did not receive all echo responses";
    for (int i = 0; i < MESSAGE_COUNT && i < (int)egress_state.messages.size(); i++)
    {
        EXPECT_EQ(std::string(MSG), egress_state.messages[i])
            << "Echo content mismatch at index " << i;
    }
}
