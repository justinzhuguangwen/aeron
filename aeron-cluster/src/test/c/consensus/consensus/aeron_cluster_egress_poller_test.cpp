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

#include <gtest/gtest.h>
#include <cstdint>
#include <cstring>

extern "C"
{
#include "aeron_cluster_egress_poller.h"
#include "aeron_cluster_configuration.h"

#include "aeron_cluster_client/messageHeader.h"
#include "aeron_cluster_client/sessionMessageHeader.h"
}

class EgressPollerTest : public ::testing::Test
{
protected:
    uint8_t m_buffer[1024]{};
    aeron_cluster_egress_poller_t *m_poller = nullptr;

    void SetUp() override
    {
        ASSERT_EQ(0, aeron_cluster_egress_poller_create(&m_poller, nullptr, 10));
    }

    void TearDown() override
    {
        if (m_poller)
        {
            aeron_cluster_egress_poller_close(m_poller);
            m_poller = nullptr;
        }
    }
};

/*
 * Test 1: Unknown message schema is ignored; poll is not complete.
 * (Java: shouldIgnoreUnknownMessageSchema)
 *
 * We encode a message header with an unknown schema ID / template ID
 * (not matching the cluster schema). The poller should return CONTINUE
 * and is_poll_complete should be false.
 */
TEST_F(EgressPollerTest, shouldIgnoreUnknownMessageSchema)
{
    /* Encode a header with a non-cluster schema ID (e.g. the archive schema). */
    struct aeron_cluster_client_messageHeader hdr;
    aeron_cluster_client_messageHeader_wrap(
        &hdr, (char *)m_buffer, 0,
        aeron_cluster_client_messageHeader_sbe_schema_version(), sizeof(m_buffer));
    /* Use a different schemaId so the template won't match cluster templates. */
    aeron_cluster_client_messageHeader_set_blockLength(&hdr, 24);
    aeron_cluster_client_messageHeader_set_templateId(&hdr, 999);
    aeron_cluster_client_messageHeader_set_schemaId(&hdr, 999);
    aeron_cluster_client_messageHeader_set_version(&hdr, 1);

    size_t length = aeron_cluster_client_messageHeader_encoded_length() + 24;

    aeron_controlled_fragment_handler_action_t action =
        aeron_cluster_egress_poller_on_fragment_for_test(m_poller, m_buffer, length);

    EXPECT_EQ(AERON_ACTION_CONTINUE, action);
    EXPECT_FALSE(m_poller->is_poll_complete);
}

/*
 * Test 2: Session message is decoded; poll completes with BREAK; second call returns ABORT.
 * (Java: shouldHandleSessionMessage)
 */
TEST_F(EgressPollerTest, shouldHandleSessionMessage)
{
    const int64_t cluster_session_id = 7777;
    const int64_t leadership_term_id = 5;

    struct aeron_cluster_client_messageHeader hdr;
    struct aeron_cluster_client_sessionMessageHeader msg;

    aeron_cluster_client_sessionMessageHeader_wrap_and_apply_header(
        &msg, (char *)m_buffer, 0, sizeof(m_buffer), &hdr);
    aeron_cluster_client_sessionMessageHeader_set_clusterSessionId(&msg, cluster_session_id);
    aeron_cluster_client_sessionMessageHeader_set_leadershipTermId(&msg, leadership_term_id);
    aeron_cluster_client_sessionMessageHeader_set_timestamp(&msg, 0);

    size_t length = aeron_cluster_client_messageHeader_encoded_length() +
                    aeron_cluster_client_sessionMessageHeader_sbe_block_length();

    /* First call: should decode and return BREAK. */
    aeron_controlled_fragment_handler_action_t action =
        aeron_cluster_egress_poller_on_fragment_for_test(m_poller, m_buffer, length);

    EXPECT_EQ(AERON_ACTION_BREAK, action);
    EXPECT_TRUE(m_poller->is_poll_complete);
    EXPECT_EQ(cluster_session_id, m_poller->cluster_session_id);
    EXPECT_EQ(leadership_term_id, m_poller->leadership_term_id);

    /* Second call: should return ABORT since poll is already complete. */
    action = aeron_cluster_egress_poller_on_fragment_for_test(m_poller, m_buffer, length);
    EXPECT_EQ(AERON_ACTION_ABORT, action);
}
