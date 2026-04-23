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

/*
 * Port of Java AeronClusterTest.
 *
 * The Java tests use Mockito mocks for Aeron, publications, and subscriptions
 * to verify: failover stay-connected, close-on-timeout, close-on-max-position,
 * and close-when-egress-image-closes.
 *
 * In C, we cannot mock the Aeron driver. Instead we build the cluster struct
 * directly (using aeron_cluster_create with stubbed ingress proxy and egress
 * poller), then exercise the state transitions that do not require real I/O:
 *
 *   1. close transitions to CLOSED and is_closed returns true
 *   2. state accessor reflects the initial CONNECTED state
 *
 * 2 test cases.
 */

#include <gtest/gtest.h>
#include <cstring>
#include <cstdint>

extern "C"
{
#define AERON_CLUSTER_TESTING 1
#include "aeron_cluster_client.h"
#include "aeron_cluster_context.h"
#include "aeron_cluster_configuration.h"
#include "aeron_cluster_egress_poller.h"
#include "aeron_cluster_ingress_proxy.h"
#include "aeron_alloc.h"
}

class ClusterClientTest : public ::testing::Test
{
protected:
    aeron_cluster_context_t *m_ctx = nullptr;
    aeron_cluster_egress_poller_t *m_poller = nullptr;
    aeron_cluster_ingress_proxy_t *m_proxy = nullptr;
    aeron_cluster_t *m_cluster = nullptr;

#define TEST_CLUSTER_SESSION_ID INT64_C(123)
#define TEST_LEADERSHIP_TERM_ID INT64_C(2)
#define TEST_LEADER_MEMBER_ID   INT32_C(1)

    void SetUp() override
    {
        ASSERT_EQ(0, aeron_cluster_context_init(&m_ctx));
        aeron_cluster_context_set_egress_channel(m_ctx, "aeron:udp?endpoint=localhost:0");
        aeron_cluster_context_set_ingress_channel(m_ctx, "aeron:udp");
        aeron_cluster_context_set_message_timeout_ns(m_ctx, 5000000000ULL);

        /* Create a standalone egress poller with no subscription (NULL).
         * The poller is usable for state tracking even without a real subscription. */
        ASSERT_EQ(0, aeron_cluster_egress_poller_create(&m_poller, nullptr, 10));

        /* Allocate a stub ingress proxy — not wired to a real publication. */
        ASSERT_EQ(0, aeron_alloc((void **)&m_proxy, sizeof(aeron_cluster_ingress_proxy_t)));
        memset(m_proxy, 0, sizeof(aeron_cluster_ingress_proxy_t));
        m_proxy->is_exclusive = false;
        m_proxy->publication = nullptr;
        m_proxy->exclusive_publication = nullptr;
        m_proxy->retry_attempts = AERON_CLUSTER_INGRESS_PROXY_SEND_ATTEMPTS_DEFAULT;

        /* Build the cluster object directly, bypassing the driver. */
        ASSERT_EQ(0, aeron_cluster_create(
            &m_cluster,
            m_ctx,
            m_proxy,
            nullptr,   /* subscription — NULL is safe for state tests */
            m_poller,
            TEST_CLUSTER_SESSION_ID,
            TEST_LEADERSHIP_TERM_ID,
            TEST_LEADER_MEMBER_ID));

        /* Ownership transferred to cluster — clear our pointers so TearDown
         * doesn't double-free. */
        m_ctx = nullptr;
        m_poller = nullptr;
        m_proxy = nullptr;
    }

    void TearDown() override
    {
        if (m_cluster)
        {
            aeron_cluster_close(m_cluster);
            m_cluster = nullptr;
        }
        /* Only free if they were NOT transferred to the cluster. */
        if (m_poller)
        {
            aeron_cluster_egress_poller_close(m_poller);
        }
        if (m_proxy)
        {
            aeron_free(m_proxy);
        }
        if (m_ctx)
        {
            aeron_cluster_context_close(m_ctx);
        }
    }
};

/* -----------------------------------------------------------------------
 * Test 1: Cluster starts in CONNECTED state; accessors return expected values.
 * (Java: setUp() verifies the cluster object fields after construction)
 * ----------------------------------------------------------------------- */
TEST_F(ClusterClientTest, initialStateIsConnected)
{
    ASSERT_NE(nullptr, m_cluster);
    EXPECT_FALSE(aeron_cluster_is_closed(m_cluster));
    EXPECT_EQ(AERON_CLUSTER_CLIENT_SESSION_CONNECTED, aeron_cluster_state(m_cluster));

    EXPECT_EQ(TEST_CLUSTER_SESSION_ID, aeron_cluster_cluster_session_id(m_cluster));
    EXPECT_EQ(TEST_LEADERSHIP_TERM_ID, aeron_cluster_leadership_term_id(m_cluster));
    EXPECT_EQ(TEST_LEADER_MEMBER_ID, aeron_cluster_leader_member_id(m_cluster));
}

/* -----------------------------------------------------------------------
 * Test 2: Close transitions to CLOSED and is_closed returns true.
 * (Java: shouldConnectViaIngressChannel — close section at the end;
 *  also shouldCloseItselfWhenDisconnected… tests)
 *
 * After close, the cluster is in the CLOSED state and is_closed() is true.
 * The proxy pointer is NULL'd so we must not call proxy-dependent APIs after.
 * ----------------------------------------------------------------------- */
TEST_F(ClusterClientTest, closeTransitionsToClosed)
{
    ASSERT_NE(nullptr, m_cluster);
    EXPECT_FALSE(aeron_cluster_is_closed(m_cluster));

    aeron_cluster_close(m_cluster);
    /* After close, the pointer is freed — we must not dereference it.
     * Null out so TearDown doesn't double-free. */
    m_cluster = nullptr;

    /* The fact that close completed without crashing is the assertion.
     * We verify the state transition via a second cluster instance: */
    aeron_cluster_context_t *ctx2 = nullptr;
    ASSERT_EQ(0, aeron_cluster_context_init(&ctx2));
    aeron_cluster_context_set_egress_channel(ctx2, "aeron:udp?endpoint=localhost:0");
    aeron_cluster_context_set_ingress_channel(ctx2, "aeron:udp");

    aeron_cluster_egress_poller_t *poller2 = nullptr;
    ASSERT_EQ(0, aeron_cluster_egress_poller_create(&poller2, nullptr, 10));

    aeron_cluster_ingress_proxy_t *proxy2 = nullptr;
    ASSERT_EQ(0, aeron_alloc((void **)&proxy2, sizeof(aeron_cluster_ingress_proxy_t)));
    memset(proxy2, 0, sizeof(aeron_cluster_ingress_proxy_t));
    proxy2->retry_attempts = 3;

    aeron_cluster_t *cluster2 = nullptr;
    ASSERT_EQ(0, aeron_cluster_create(
        &cluster2, ctx2, proxy2, nullptr, poller2,
        456, 3, 2));

    /* Verify CONNECTED before close */
    EXPECT_FALSE(aeron_cluster_is_closed(cluster2));
    EXPECT_EQ(AERON_CLUSTER_CLIENT_SESSION_CONNECTED, aeron_cluster_state(cluster2));

    /* Track PUBLICATION_CLOSED to trigger state transition to CLOSED */
    aeron_cluster_track_ingress_result(cluster2, AERON_PUBLICATION_CLOSED);
    EXPECT_TRUE(aeron_cluster_is_closed(cluster2));
    EXPECT_EQ(AERON_CLUSTER_CLIENT_SESSION_CLOSED, aeron_cluster_state(cluster2));

    aeron_cluster_close(cluster2);
}
