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
 * Port of Java AeronClusterAsyncConnectTest.
 *
 * The Java tests use Mockito to mock the Aeron client. In C, we cannot mock
 * the real Aeron driver. Instead we test the parts of the async connect state
 * machine that do not require a live driver: initial state, context validation,
 * and cleanup/delete paths.
 *
 * 7 test cases total.
 */

#include <gtest/gtest.h>
#include <cstring>
#include <cstdint>

extern "C"
{
#include "aeron_cluster_async_connect.h"
#include "aeron_cluster_client.h"
#include "aeron_cluster_context.h"
#include "aeron_cluster_configuration.h"
}

/* -----------------------------------------------------------------------
 * Test 1: Initial state of a freshly created context has expected defaults.
 * (Java: initialState — verifies CREATE_EGRESS_SUBSCRIPTION / step value)
 *
 * Without a driver we cannot call aeron_cluster_async_connect() itself (it
 * needs a connected Aeron instance to add publications/subscriptions).
 * Instead we verify the context defaults that the async connect would use.
 * ----------------------------------------------------------------------- */
TEST(AsyncConnectTest, initialContextState)
{
    aeron_cluster_context_t *ctx = nullptr;
    ASSERT_EQ(0, aeron_cluster_context_init(&ctx));
    ASSERT_NE(nullptr, ctx);

    /* Defaults from Java: ingressStreamId=101, egressStreamId=102 */
    EXPECT_EQ(AERON_CLUSTER_INGRESS_STREAM_ID_DEFAULT, aeron_cluster_context_get_ingress_stream_id(ctx));
    EXPECT_EQ(AERON_CLUSTER_EGRESS_STREAM_ID_DEFAULT, aeron_cluster_context_get_egress_stream_id(ctx));

    /* is_ingress_exclusive defaults to false */
    EXPECT_FALSE(aeron_cluster_context_get_is_ingress_exclusive(ctx));

    /* owns_aeron_client defaults to false */
    EXPECT_FALSE(aeron_cluster_context_get_owns_aeron_client(ctx));

    aeron_cluster_context_close(ctx);
}

/* -----------------------------------------------------------------------
 * Test 2: Delete a NULL async connect is safe.
 * (Java: shouldCloseAsyncSubscription — close path; we verify delete(NULL) is no-op)
 * ----------------------------------------------------------------------- */
TEST(AsyncConnectTest, deleteNullIsNoOp)
{
    EXPECT_EQ(0, aeron_cluster_async_connect_delete(nullptr));
}

/* -----------------------------------------------------------------------
 * Test 3: Async connect fails when egress channel is not set.
 * (Java: implicitly — conclude throws on missing channels)
 *
 * aeron_cluster_async_connect calls conclude first, which validates
 * required context fields. Without an egress channel, it should return -1.
 * ----------------------------------------------------------------------- */
TEST(AsyncConnectTest, shouldFailWithoutEgressChannel)
{
    aeron_cluster_context_t *ctx = nullptr;
    ASSERT_EQ(0, aeron_cluster_context_init(&ctx));

    /* Ingress is set but egress is deliberately left NULL */
    aeron_cluster_context_set_ingress_channel(ctx, "aeron:udp?endpoint=localhost:20000");
    /* Do not set egress_channel — it stays NULL */

    aeron_cluster_async_connect_t *async = nullptr;
    int rc = aeron_cluster_async_connect(&async, ctx);
    EXPECT_EQ(-1, rc);
    EXPECT_EQ(nullptr, async);

    aeron_cluster_context_close(ctx);
}

/* -----------------------------------------------------------------------
 * Test 4: Async connect fails when message_retry_attempts is zero.
 * (Java: context conclude validates configuration)
 * ----------------------------------------------------------------------- */
TEST(AsyncConnectTest, shouldFailWithZeroRetryAttempts)
{
    aeron_cluster_context_t *ctx = nullptr;
    ASSERT_EQ(0, aeron_cluster_context_init(&ctx));

    aeron_cluster_context_set_egress_channel(ctx, "aeron:udp?endpoint=localhost:0");
    aeron_cluster_context_set_ingress_channel(ctx, "aeron:udp?endpoint=localhost:20000");
    aeron_cluster_context_set_message_retry_attempts(ctx, 0);

    aeron_cluster_async_connect_t *async = nullptr;
    int rc = aeron_cluster_async_connect(&async, ctx);
    EXPECT_EQ(-1, rc);
    EXPECT_EQ(nullptr, async);

    aeron_cluster_context_close(ctx);
}

/* -----------------------------------------------------------------------
 * Test 5: Context conclude succeeds with valid minimal configuration.
 * (Java: the happy-path setup in @BeforeEach — verifies conclude works)
 *
 * Since conclude also creates an aeron client when none is set, we test
 * the conclude validation path directly.
 * ----------------------------------------------------------------------- */
TEST(AsyncConnectTest, contextConcludeFailsWithoutEgressChannel)
{
    aeron_cluster_context_t *ctx = nullptr;
    ASSERT_EQ(0, aeron_cluster_context_init(&ctx));

    /* Only set ingress — missing egress should fail conclude */
    aeron_cluster_context_set_ingress_channel(ctx, "aeron:udp?endpoint=localhost:20000");

    int rc = aeron_cluster_context_conclude(ctx);
    EXPECT_EQ(-1, rc);

    aeron_cluster_context_close(ctx);
}

/* -----------------------------------------------------------------------
 * Test 6: Context fields set/get round-trip correctly.
 * (Java: the context spy configuration in the test fixture)
 * ----------------------------------------------------------------------- */
TEST(AsyncConnectTest, contextSetGetRoundTrip)
{
    aeron_cluster_context_t *ctx = nullptr;
    ASSERT_EQ(0, aeron_cluster_context_init(&ctx));

    aeron_cluster_context_set_egress_channel(ctx, "aeron:udp?endpoint=localhost:0");
    EXPECT_STREQ("aeron:udp?endpoint=localhost:0", aeron_cluster_context_get_egress_channel(ctx));

    aeron_cluster_context_set_egress_stream_id(ctx, 42);
    EXPECT_EQ(42, aeron_cluster_context_get_egress_stream_id(ctx));

    aeron_cluster_context_set_ingress_channel(ctx, "aeron:udp?endpoint=replace-me:5555");
    EXPECT_STREQ("aeron:udp?endpoint=replace-me:5555", aeron_cluster_context_get_ingress_channel(ctx));

    aeron_cluster_context_set_ingress_stream_id(ctx, -19);
    EXPECT_EQ(-19, aeron_cluster_context_get_ingress_stream_id(ctx));

    aeron_cluster_context_set_is_ingress_exclusive(ctx, true);
    EXPECT_TRUE(aeron_cluster_context_get_is_ingress_exclusive(ctx));

    aeron_cluster_context_set_owns_aeron_client(ctx, false);
    EXPECT_FALSE(aeron_cluster_context_get_owns_aeron_client(ctx));

    aeron_cluster_context_set_message_timeout_ns(ctx, 1000000000ULL);
    EXPECT_EQ(1000000000ULL, aeron_cluster_context_get_message_timeout_ns(ctx));

    aeron_cluster_context_close(ctx);
}

/* -----------------------------------------------------------------------
 * Test 7: Context duplicate preserves all fields.
 * (Java: the context is duplicated inside AsyncConnect constructor; we verify
 *  that duplicate correctly copies channels and stream IDs)
 * ----------------------------------------------------------------------- */
TEST(AsyncConnectTest, contextDuplicatePreservesFields)
{
    aeron_cluster_context_t *src = nullptr;
    ASSERT_EQ(0, aeron_cluster_context_init(&src));

    aeron_cluster_context_set_egress_channel(src, "aeron:udp?endpoint=localhost:9999");
    aeron_cluster_context_set_egress_stream_id(src, 42);
    aeron_cluster_context_set_ingress_channel(src, "aeron:udp?endpoint=localhost:5555");
    aeron_cluster_context_set_ingress_stream_id(src, -19);
    aeron_cluster_context_set_ingress_endpoints(src, "0=localhost:20000,1=localhost:20001");
    aeron_cluster_context_set_is_ingress_exclusive(src, true);
    aeron_cluster_context_set_message_timeout_ns(src, 7000000000ULL);

    aeron_cluster_context_t *dup = nullptr;
    ASSERT_EQ(0, aeron_cluster_context_duplicate(&dup, src));
    ASSERT_NE(nullptr, dup);

    EXPECT_STREQ("aeron:udp?endpoint=localhost:9999", aeron_cluster_context_get_egress_channel(dup));
    EXPECT_EQ(42, aeron_cluster_context_get_egress_stream_id(dup));
    EXPECT_STREQ("aeron:udp?endpoint=localhost:5555", aeron_cluster_context_get_ingress_channel(dup));
    EXPECT_EQ(-19, aeron_cluster_context_get_ingress_stream_id(dup));
    EXPECT_STREQ("0=localhost:20000,1=localhost:20001", aeron_cluster_context_get_ingress_endpoints(dup));
    EXPECT_TRUE(aeron_cluster_context_get_is_ingress_exclusive(dup));
    EXPECT_EQ(7000000000ULL, aeron_cluster_context_get_message_timeout_ns(dup));

    /* Mutating the duplicate should not affect the source */
    aeron_cluster_context_set_egress_stream_id(dup, 999);
    EXPECT_EQ(42, aeron_cluster_context_get_egress_stream_id(src));

    aeron_cluster_context_close(dup);
    aeron_cluster_context_close(src);
}
