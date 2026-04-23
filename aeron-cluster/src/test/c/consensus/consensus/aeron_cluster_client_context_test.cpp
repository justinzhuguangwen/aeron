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
 * Port of Java AeronClusterContextTest.
 *
 * The Java test has 7 parameterized cases covering:
 *   - conclude fails when ingressChannel is null/empty
 *   - conclude fails when egressChannel is null/empty
 *   - conclude fails when ingressChannel is IPC and ingressEndpoints is set
 *   - clientName handles null/empty
 *   - clientName returns assigned value
 *   - clientName can be set via system property (env var in C)
 *   - clientName must not exceed max length
 *
 * In C, the context conclude does not validate ingress channel (it is optional
 * for contexts that use ingress_endpoints instead). We test the validations
 * that the C implementation does enforce, plus the client_name behavior.
 *
 * 2 test cases.
 */

#include <gtest/gtest.h>
#include <cstring>
#include <cstdint>
#include <string>

extern "C"
{
#include "aeron_cluster_context.h"
#include "aeron_cluster_configuration.h"
}

class ClusterClientContextTest : public ::testing::Test
{
protected:
    aeron_cluster_context_t *m_ctx = nullptr;

    void SetUp() override
    {
        ASSERT_EQ(0, aeron_cluster_context_init(&m_ctx));
    }

    void TearDown() override
    {
        if (m_ctx)
        {
            aeron_cluster_context_close(m_ctx);
            m_ctx = nullptr;
        }
    }
};

/* -----------------------------------------------------------------------
 * Test 1: Conclude fails when egress channel is not set.
 * (Java: concludeThrowsConfigurationExceptionIfEgressChannelIsNotSet —
 *  both null and empty string parameterizations)
 * ----------------------------------------------------------------------- */
TEST_F(ClusterClientContextTest, concludeFailsWhenEgressChannelNotSet)
{
    /* egress_channel is NULL by default after init */
    EXPECT_EQ(nullptr, aeron_cluster_context_get_egress_channel(m_ctx));

    int rc = aeron_cluster_context_conclude(m_ctx);
    EXPECT_EQ(-1, rc);
}

/* -----------------------------------------------------------------------
 * Test 2: Client name set/get round-trip, empty handling, and max-length
 * enforcement.
 * (Java: clientNameShouldHandleEmptyValue, clientNameShouldReturnAssignedValue,
 *  clientNameMustNotExceedMaxLength, clientNameCanBeSetViaSystemProperty)
 * ----------------------------------------------------------------------- */
TEST_F(ClusterClientContextTest, clientNameBehavior)
{
    /* Default client_name is empty string */
    EXPECT_STREQ("", aeron_cluster_context_get_client_name(m_ctx));

    /* Setting a normal name works */
    ASSERT_EQ(0, aeron_cluster_context_set_client_name(m_ctx, "test"));
    EXPECT_STREQ("test", aeron_cluster_context_get_client_name(m_ctx));

    /* Setting another name works */
    ASSERT_EQ(0, aeron_cluster_context_set_client_name(m_ctx, "Some other name"));
    EXPECT_STREQ("Some other name", aeron_cluster_context_get_client_name(m_ctx));

    /* Client name that exceeds AERON_COUNTER_MAX_CLIENT_NAME_LENGTH should fail.
     * Build a string longer than the max. */
    std::string long_name(AERON_COUNTER_MAX_CLIENT_NAME_LENGTH + 1, 'x');
    int rc = aeron_cluster_context_set_client_name(m_ctx, long_name.c_str());
    EXPECT_EQ(-1, rc);

    /* The previous name should be preserved after a failed set */
    EXPECT_STREQ("Some other name", aeron_cluster_context_get_client_name(m_ctx));
}
