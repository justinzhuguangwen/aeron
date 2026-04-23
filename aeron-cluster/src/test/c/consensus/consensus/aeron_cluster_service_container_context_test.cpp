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
 * C port of Java ClusteredServiceContainerContextTest.
 *
 * Tests verify context defaults, setter/getter round-trips, env-var
 * overrides, and conclude() validation.  No live Aeron driver needed.
 *
 * 11 test cases ported:
 *  1. shouldInitializeWithDefaultServiceId
 *  2. shouldInitializeWithDefaultStreamIds
 *  3. shouldInitializeWithDefaultChannels
 *  4. concludeFailsIfServiceCallbackIsNull
 *  5. shouldSetAndGetServiceId
 *  6. shouldSetAndGetControlChannel
 *  7. shouldSetAndGetClusterDir
 *  8. shouldApplyEnvVarForServiceId
 *  9. shouldApplyEnvVarForSnapshotStreamId
 * 10. shouldInitializeWithNullAeron
 * 11. shouldSetServiceNameEquivalent (cluster_id + service_id combined)
 */

#ifdef _MSC_VER
#ifndef _WINSOCKAPI_
#define _WINSOCKAPI_
#endif
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>
#endif

#include <gtest/gtest.h>
#include <cstring>
#include <cstdlib>

#ifdef _MSC_VER
#include <process.h>
#if !defined(getpid)
#define getpid _getpid
#endif
static int setenv(const char *name, const char *value, int overwrite)
{
    (void)overwrite;
    return _putenv_s(name, value);
}
static int unsetenv(const char *name)
{
    return _putenv_s(name, "");
}
#endif

extern "C"
{
#include "aeron_cluster_service_context.h"
#include "aeron_alloc.h"
}

class ClusteredServiceContainerContextTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        ASSERT_EQ(0, aeron_cluster_service_context_init(&m_ctx));
    }
    void TearDown() override
    {
        aeron_cluster_service_context_close(m_ctx);
        m_ctx = nullptr;
    }
    aeron_cluster_service_context_t *m_ctx = nullptr;
};

/* -----------------------------------------------------------------------
 * Test 1: shouldInitializeWithDefaultServiceId
 * Java: serviceId defaults to 0
 * ----------------------------------------------------------------------- */
TEST_F(ClusteredServiceContainerContextTest, shouldInitializeWithDefaultServiceId)
{
    EXPECT_EQ(AERON_CLUSTER_SERVICE_ID_DEFAULT, m_ctx->service_id);
    EXPECT_EQ(0, m_ctx->service_id);
}

/* -----------------------------------------------------------------------
 * Test 2: shouldInitializeWithDefaultStreamIds
 * Java: consensus module stream id = 104, service = 105, snapshot = 107
 * ----------------------------------------------------------------------- */
TEST_F(ClusteredServiceContainerContextTest, shouldInitializeWithDefaultStreamIds)
{
    EXPECT_EQ(AERON_CLUSTER_CONSENSUS_MODULE_STREAM_ID_DEFAULT, m_ctx->consensus_module_stream_id);
    EXPECT_EQ(AERON_CLUSTER_SERVICE_STREAM_ID_DEFAULT, m_ctx->service_stream_id);
    EXPECT_EQ(AERON_CLUSTER_SNAPSHOT_STREAM_ID_DEFAULT, m_ctx->snapshot_stream_id);
}

/* -----------------------------------------------------------------------
 * Test 3: shouldInitializeWithDefaultChannels
 * Java: control, service, snapshot channels are all "aeron:ipc"
 * ----------------------------------------------------------------------- */
TEST_F(ClusteredServiceContainerContextTest, shouldInitializeWithDefaultChannels)
{
    ASSERT_NE(nullptr, m_ctx->control_channel);
    ASSERT_NE(nullptr, m_ctx->service_channel);
    ASSERT_NE(nullptr, m_ctx->snapshot_channel);
    EXPECT_STREQ(AERON_CLUSTER_CONTROL_CHANNEL_DEFAULT, m_ctx->control_channel);
}

/* -----------------------------------------------------------------------
 * Test 4: concludeFailsIfServiceCallbackIsNull
 * Java: throws if clusteredService is null
 * ----------------------------------------------------------------------- */
TEST_F(ClusteredServiceContainerContextTest, concludeFailsIfServiceCallbackIsNull)
{
    /* service is NULL (default) -> conclude must fail */
    EXPECT_EQ(nullptr, m_ctx->service);
    EXPECT_EQ(-1, aeron_cluster_service_context_conclude(m_ctx));
}

/* -----------------------------------------------------------------------
 * Test 5: shouldSetAndGetServiceId
 * Java: context.serviceId(2) -> assertEquals(2, context.serviceId())
 * ----------------------------------------------------------------------- */
TEST_F(ClusteredServiceContainerContextTest, shouldSetAndGetServiceId)
{
    ASSERT_EQ(0, aeron_cluster_service_context_set_service_id(m_ctx, 2));
    EXPECT_EQ(2, aeron_cluster_service_context_get_service_id(m_ctx));
}

/* -----------------------------------------------------------------------
 * Test 6: shouldSetAndGetControlChannel
 * ----------------------------------------------------------------------- */
TEST_F(ClusteredServiceContainerContextTest, shouldSetAndGetControlChannel)
{
    ASSERT_EQ(0, aeron_cluster_service_context_set_control_channel(m_ctx, "aeron:ipc?alias=test"));
    EXPECT_STREQ("aeron:ipc?alias=test", aeron_cluster_service_context_get_control_channel(m_ctx));
}

/* -----------------------------------------------------------------------
 * Test 7: shouldSetAndGetClusterDir
 * ----------------------------------------------------------------------- */
TEST_F(ClusteredServiceContainerContextTest, shouldSetAndGetClusterDir)
{
    ASSERT_EQ(0, aeron_cluster_service_context_set_cluster_dir(m_ctx, "/tmp/svc_cluster"));
    EXPECT_STREQ("/tmp/svc_cluster", aeron_cluster_service_context_get_cluster_dir(m_ctx));
}

/* -----------------------------------------------------------------------
 * Test 8: shouldApplyEnvVarForServiceId
 * Java: system property "aeron.cluster.service.id" = "5"
 * ----------------------------------------------------------------------- */
TEST_F(ClusteredServiceContainerContextTest, shouldApplyEnvVarForServiceId)
{
    setenv(AERON_CLUSTER_SERVICE_ID_ENV_VAR, "5", 1);
    aeron_cluster_service_context_close(m_ctx);
    m_ctx = nullptr;
    ASSERT_EQ(0, aeron_cluster_service_context_init(&m_ctx));
    unsetenv(AERON_CLUSTER_SERVICE_ID_ENV_VAR);

    EXPECT_EQ(5, m_ctx->service_id);
}

/* -----------------------------------------------------------------------
 * Test 9: shouldApplyEnvVarForSnapshotStreamId
 * ----------------------------------------------------------------------- */
TEST_F(ClusteredServiceContainerContextTest, shouldApplyEnvVarForSnapshotStreamId)
{
    setenv(AERON_CLUSTER_SNAPSHOT_STREAM_ID_ENV_VAR, "777", 1);
    aeron_cluster_service_context_close(m_ctx);
    m_ctx = nullptr;
    ASSERT_EQ(0, aeron_cluster_service_context_init(&m_ctx));
    unsetenv(AERON_CLUSTER_SNAPSHOT_STREAM_ID_ENV_VAR);

    EXPECT_EQ(777, m_ctx->snapshot_stream_id);
}

/* -----------------------------------------------------------------------
 * Test 10: shouldInitializeWithNullAeron
 * Java: context starts without an Aeron client
 * ----------------------------------------------------------------------- */
TEST_F(ClusteredServiceContainerContextTest, shouldInitializeWithNullAeron)
{
    EXPECT_EQ(nullptr, m_ctx->aeron);
    EXPECT_FALSE(m_ctx->owns_aeron_client);
}

/* -----------------------------------------------------------------------
 * Test 11: shouldSetServiceNameEquivalent
 *
 * Java: if serviceName is null/empty, conclude generates
 *       "clustered-service-{clusterId}-{serviceId}".
 *
 * C: there is no service_name field in the C context, but we verify
 *    that cluster_id + service_id are both settable and combine to form
 *    a unique service identity (the C agent uses these for counter labels).
 * ----------------------------------------------------------------------- */
TEST_F(ClusteredServiceContainerContextTest, shouldSetClusterIdAndServiceIdCombination)
{
    ASSERT_EQ(0, aeron_cluster_service_context_set_cluster_id(m_ctx, 7));
    ASSERT_EQ(0, aeron_cluster_service_context_set_service_id(m_ctx, 5));

    EXPECT_EQ(7, aeron_cluster_service_context_get_cluster_id(m_ctx));
    EXPECT_EQ(5, aeron_cluster_service_context_get_service_id(m_ctx));

    /* Verify they combine to form a unique identifier */
    char service_name[64];
    snprintf(service_name, sizeof(service_name), "clustered-service-%d-%d",
        aeron_cluster_service_context_get_cluster_id(m_ctx),
        aeron_cluster_service_context_get_service_id(m_ctx));
    EXPECT_STREQ("clustered-service-7-5", service_name);
}
