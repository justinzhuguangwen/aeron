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
#include <string>
#include <vector>

extern "C"
{
#include "aeron_cluster_egress_adapter.h"
#include "aeron_cluster_configuration.h"

#include "aeron_cluster_client/messageHeader.h"
#include "aeron_cluster_client/sessionMessageHeader.h"
#include "aeron_cluster_client/sessionEvent.h"
#include "aeron_cluster_client/newLeaderEvent.h"
#include "aeron_cluster_client/adminResponse.h"
#include "aeron_cluster_client/adminRequestType.h"
#include "aeron_cluster_client/adminResponseCode.h"
#include "aeron_cluster_client/eventCode.h"
}

struct OnMessageRecord
{
    int64_t cluster_session_id;
    int64_t leadership_term_id;
    int64_t timestamp;
    std::vector<uint8_t> payload;
};

struct OnSessionEventRecord
{
    int64_t correlation_id;
    int64_t cluster_session_id;
    int64_t leadership_term_id;
    int32_t leader_member_id;
    int32_t event_code;
    std::string detail;
};

struct OnNewLeaderRecord
{
    int64_t cluster_session_id;
    int64_t leadership_term_id;
    int32_t leader_member_id;
    std::string ingress_endpoints;
};

struct OnAdminResponseRecord
{
    int64_t cluster_session_id;
    int64_t correlation_id;
    int32_t request_type;
    int32_t response_code;
    std::string message;
    std::vector<uint8_t> payload;
};

static std::vector<OnMessageRecord> g_messages;
static std::vector<OnSessionEventRecord> g_session_events;
static std::vector<OnNewLeaderRecord> g_new_leaders;
static std::vector<OnAdminResponseRecord> g_admin_responses;

static void on_message_cb(
    void *clientd,
    int64_t cluster_session_id,
    int64_t leadership_term_id,
    int64_t timestamp,
    const uint8_t *buffer, size_t length,
    aeron_header_t *header)
{
    OnMessageRecord rec;
    rec.cluster_session_id = cluster_session_id;
    rec.leadership_term_id = leadership_term_id;
    rec.timestamp = timestamp;
    rec.payload.assign(buffer, buffer + length);
    g_messages.push_back(rec);
}

static void on_session_event_cb(
    void *clientd,
    int64_t correlation_id,
    int64_t cluster_session_id,
    int64_t leadership_term_id,
    int32_t leader_member_id,
    int32_t event_code,
    const char *detail, size_t detail_length)
{
    OnSessionEventRecord rec;
    rec.correlation_id = correlation_id;
    rec.cluster_session_id = cluster_session_id;
    rec.leadership_term_id = leadership_term_id;
    rec.leader_member_id = leader_member_id;
    rec.event_code = event_code;
    rec.detail = std::string(detail, detail_length);
    g_session_events.push_back(rec);
}

static void on_new_leader_cb(
    void *clientd,
    int64_t cluster_session_id,
    int64_t leadership_term_id,
    int32_t leader_member_id,
    const char *ingress_endpoints, size_t ingress_endpoints_length)
{
    OnNewLeaderRecord rec;
    rec.cluster_session_id = cluster_session_id;
    rec.leadership_term_id = leadership_term_id;
    rec.leader_member_id = leader_member_id;
    rec.ingress_endpoints = std::string(ingress_endpoints, ingress_endpoints_length);
    g_new_leaders.push_back(rec);
}

static void on_admin_response_cb(
    void *clientd,
    int64_t cluster_session_id,
    int64_t correlation_id,
    int32_t request_type,
    int32_t response_code,
    const char *message, size_t message_length,
    const uint8_t *payload, size_t payload_length)
{
    OnAdminResponseRecord rec;
    rec.cluster_session_id = cluster_session_id;
    rec.correlation_id = correlation_id;
    rec.request_type = request_type;
    rec.response_code = response_code;
    rec.message = std::string(message, message_length);
    rec.payload.assign(payload, payload + payload_length);
    g_admin_responses.push_back(rec);
}

class EgressAdapterTest : public ::testing::Test
{
protected:
    uint8_t m_buffer[1024]{};
    aeron_cluster_egress_adapter_t *m_adapter = nullptr;
    aeron_cluster_egress_listener_t m_listener{};

    void SetUp() override
    {
        g_messages.clear();
        g_session_events.clear();
        g_new_leaders.clear();
        g_admin_responses.clear();

        memset(&m_listener, 0, sizeof(m_listener));
        m_listener.on_message = on_message_cb;
        m_listener.on_session_event = on_session_event_cb;
        m_listener.on_new_leader = on_new_leader_cb;
        m_listener.on_admin_response = on_admin_response_cb;
        m_listener.clientd = nullptr;
    }

    void TearDown() override
    {
        if (m_adapter)
        {
            aeron_cluster_egress_adapter_close(m_adapter);
            m_adapter = nullptr;
        }
    }

    void createAdapter(int64_t session_id)
    {
        ASSERT_EQ(0, aeron_cluster_egress_adapter_create(&m_adapter, nullptr, &m_listener, session_id));
    }

    size_t encodeSessionMessage(int64_t session_id, int64_t timestamp, const uint8_t *payload, size_t payload_len)
    {
        struct aeron_cluster_client_messageHeader hdr;
        struct aeron_cluster_client_sessionMessageHeader msg;

        aeron_cluster_client_sessionMessageHeader_wrap_and_apply_header(
            &msg, (char *)m_buffer, 0, sizeof(m_buffer), &hdr);
        aeron_cluster_client_sessionMessageHeader_set_clusterSessionId(&msg, session_id);
        aeron_cluster_client_sessionMessageHeader_set_timestamp(&msg, timestamp);
        aeron_cluster_client_sessionMessageHeader_set_leadershipTermId(&msg, 0);

        size_t total = AERON_CLUSTER_SESSION_HEADER_LENGTH;
        if (payload && payload_len > 0)
        {
            memcpy(m_buffer + total, payload, payload_len);
            total += payload_len;
        }
        return total;
    }

    size_t encodeSessionEvent(
        int64_t cluster_session_id, int64_t correlation_id, int64_t leadership_term_id,
        int32_t leader_member_id, enum aeron_cluster_client_eventCode code,
        int32_t version, const char *detail)
    {
        struct aeron_cluster_client_messageHeader hdr;
        struct aeron_cluster_client_sessionEvent msg;

        aeron_cluster_client_sessionEvent_wrap_and_apply_header(
            &msg, (char *)m_buffer, 0, sizeof(m_buffer), &hdr);
        aeron_cluster_client_sessionEvent_set_clusterSessionId(&msg, cluster_session_id);
        aeron_cluster_client_sessionEvent_set_correlationId(&msg, correlation_id);
        aeron_cluster_client_sessionEvent_set_leadershipTermId(&msg, leadership_term_id);
        aeron_cluster_client_sessionEvent_set_leaderMemberId(&msg, leader_member_id);
        aeron_cluster_client_sessionEvent_set_code(&msg, code);
        aeron_cluster_client_sessionEvent_set_version(&msg, version);
        aeron_cluster_client_sessionEvent_put_detail(&msg, detail, (uint32_t)strlen(detail));

        return aeron_cluster_client_messageHeader_encoded_length() +
               aeron_cluster_client_sessionEvent_encoded_length(&msg);
    }

    size_t encodeNewLeaderEvent(
        int64_t cluster_session_id, int64_t leadership_term_id,
        int32_t leader_member_id, const char *ingress_endpoints)
    {
        struct aeron_cluster_client_messageHeader hdr;
        struct aeron_cluster_client_newLeaderEvent msg;

        aeron_cluster_client_newLeaderEvent_wrap_and_apply_header(
            &msg, (char *)m_buffer, 0, sizeof(m_buffer), &hdr);
        aeron_cluster_client_newLeaderEvent_set_leadershipTermId(&msg, leadership_term_id);
        aeron_cluster_client_newLeaderEvent_set_clusterSessionId(&msg, cluster_session_id);
        aeron_cluster_client_newLeaderEvent_set_leaderMemberId(&msg, leader_member_id);
        aeron_cluster_client_newLeaderEvent_put_ingressEndpoints(
            &msg, ingress_endpoints, (uint32_t)strlen(ingress_endpoints));

        return aeron_cluster_client_messageHeader_encoded_length() +
               aeron_cluster_client_newLeaderEvent_encoded_length(&msg);
    }

    size_t encodeAdminResponse(
        int64_t cluster_session_id, int64_t correlation_id,
        enum aeron_cluster_client_adminRequestType request_type,
        enum aeron_cluster_client_adminResponseCode response_code,
        const char *message, const uint8_t *payload, size_t payload_len)
    {
        struct aeron_cluster_client_messageHeader hdr;
        struct aeron_cluster_client_adminResponse msg;

        aeron_cluster_client_adminResponse_wrap_and_apply_header(
            &msg, (char *)m_buffer, 0, sizeof(m_buffer), &hdr);
        aeron_cluster_client_adminResponse_set_clusterSessionId(&msg, cluster_session_id);
        aeron_cluster_client_adminResponse_set_correlationId(&msg, correlation_id);
        aeron_cluster_client_adminResponse_set_requestType(&msg, request_type);
        aeron_cluster_client_adminResponse_set_responseCode(&msg, response_code);
        aeron_cluster_client_adminResponse_put_message(&msg, message, (uint32_t)strlen(message));
        aeron_cluster_client_adminResponse_put_payload(&msg, (const char *)payload, (uint32_t)payload_len);

        return aeron_cluster_client_messageHeader_encoded_length() +
               aeron_cluster_client_adminResponse_encoded_length(&msg);
    }
};

/*
 * Test 1: Session message dispatches on_message when session ID matches.
 * (Java: onFragmentShouldInvokeOnMessageCallbackIfSessionIdMatches)
 */
TEST_F(EgressAdapterTest, shouldInvokeOnMessageWhenSessionIdMatches)
{
    const int64_t session_id = 2973438724L;
    const int64_t timestamp = -46328746238764832L;
    createAdapter(session_id);

    uint8_t payload[] = {0xDE, 0xAD, 0xBE, 0xEF};
    size_t length = encodeSessionMessage(session_id, timestamp, payload, sizeof(payload));

    aeron_cluster_egress_adapter_on_fragment_for_test(m_adapter, m_buffer, length, nullptr);

    ASSERT_EQ(1u, g_messages.size());
    EXPECT_EQ(session_id, g_messages[0].cluster_session_id);
    EXPECT_EQ(timestamp, g_messages[0].timestamp);
    EXPECT_EQ(sizeof(payload), g_messages[0].payload.size());
}

/*
 * Test 2: Session message is a no-op when session ID does NOT match.
 * (Java: onFragmentIsANoOpIfSessionIdDoesNotMatchOnSessionMessage)
 */
TEST_F(EgressAdapterTest, shouldIgnoreSessionMessageWhenSessionIdDoesNotMatch)
{
    const int64_t session_id = 21;
    createAdapter(-19);

    uint8_t payload[] = {0x01};
    size_t length = encodeSessionMessage(session_id, 1000, payload, sizeof(payload));

    aeron_cluster_egress_adapter_on_fragment_for_test(m_adapter, m_buffer, length, nullptr);

    EXPECT_EQ(0u, g_messages.size());
}

/*
 * Test 3: Session event dispatches on_session_event.
 * (Java: onFragmentShouldInvokeOnSessionEventCallbackIfSessionIdMatches)
 */
TEST_F(EgressAdapterTest, shouldInvokeOnSessionEvent)
{
    const int64_t cluster_session_id = 42;
    const int64_t correlation_id = 777;
    const int64_t leadership_term_id = 6;
    const int32_t leader_member_id = 3;
    const char *detail = "Event details";
    createAdapter(cluster_session_id);

    size_t length = encodeSessionEvent(
        cluster_session_id, correlation_id, leadership_term_id,
        leader_member_id, aeron_cluster_client_eventCode_REDIRECT, 18, detail);

    aeron_cluster_egress_adapter_on_fragment_for_test(m_adapter, m_buffer, length, nullptr);

    ASSERT_EQ(1u, g_session_events.size());
    EXPECT_EQ(correlation_id, g_session_events[0].correlation_id);
    EXPECT_EQ(cluster_session_id, g_session_events[0].cluster_session_id);
    EXPECT_EQ(leadership_term_id, g_session_events[0].leadership_term_id);
    EXPECT_EQ(leader_member_id, g_session_events[0].leader_member_id);
    EXPECT_EQ((int32_t)aeron_cluster_client_eventCode_REDIRECT, g_session_events[0].event_code);
    EXPECT_EQ(std::string(detail), g_session_events[0].detail);
}

/*
 * Test 4: Session event dispatches on_session_event with different session ID.
 * (The C adapter does NOT filter session events by session ID, unlike Java.)
 * Verify that the adapter still dispatches the callback regardless.
 */
TEST_F(EgressAdapterTest, shouldDispatchSessionEventRegardlessOfSessionId)
{
    const int64_t cluster_session_id = 42;
    createAdapter(cluster_session_id + 1);

    size_t length = encodeSessionEvent(
        cluster_session_id, 777, 6, 3,
        aeron_cluster_client_eventCode_REDIRECT, 18, "Event details");

    aeron_cluster_egress_adapter_on_fragment_for_test(m_adapter, m_buffer, length, nullptr);

    /* C adapter does not filter session events by session ID -- dispatch happens. */
    EXPECT_EQ(1u, g_session_events.size());
}

/*
 * Test 5: New leader event dispatches on_new_leader when session ID matches.
 * (Java: onFragmentShouldInvokeOnNewLeaderCallbackIfSessionIdMatches)
 */
TEST_F(EgressAdapterTest, shouldInvokeOnNewLeader)
{
    const int64_t cluster_session_id = 0;
    const int64_t leadership_term_id = 6;
    const int32_t leader_member_id = 9999;
    const char *endpoints = "ingress endpoints ...";
    createAdapter(cluster_session_id);

    size_t length = encodeNewLeaderEvent(
        cluster_session_id, leadership_term_id, leader_member_id, endpoints);

    aeron_cluster_egress_adapter_on_fragment_for_test(m_adapter, m_buffer, length, nullptr);

    ASSERT_EQ(1u, g_new_leaders.size());
    EXPECT_EQ(cluster_session_id, g_new_leaders[0].cluster_session_id);
    EXPECT_EQ(leadership_term_id, g_new_leaders[0].leadership_term_id);
    EXPECT_EQ(leader_member_id, g_new_leaders[0].leader_member_id);
    EXPECT_EQ(std::string(endpoints), g_new_leaders[0].ingress_endpoints);
}

/*
 * Test 6: New leader event dispatches on_new_leader with different session ID.
 * (C adapter does NOT filter new leader events by session ID.)
 */
TEST_F(EgressAdapterTest, shouldDispatchNewLeaderEventRegardlessOfSessionId)
{
    createAdapter(0);

    size_t length = encodeNewLeaderEvent(-100, 6, 9999, "ingress endpoints ...");

    aeron_cluster_egress_adapter_on_fragment_for_test(m_adapter, m_buffer, length, nullptr);

    EXPECT_EQ(1u, g_new_leaders.size());
}

/*
 * Test 7: Admin response dispatches on_admin_response.
 * (Java: onFragmentShouldInvokeOnAdminResponseCallbackIfSessionIdMatches)
 */
TEST_F(EgressAdapterTest, shouldInvokeOnAdminResponse)
{
    const int64_t cluster_session_id = 18;
    const int64_t correlation_id = INT64_C(3274239749237498239);
    const char *msg_text = "Unauthorised access detected!";
    uint8_t payload[] = {0x1, 0x2, 0x3};
    createAdapter(cluster_session_id);

    size_t length = encodeAdminResponse(
        cluster_session_id, correlation_id,
        aeron_cluster_client_adminRequestType_SNAPSHOT,
        aeron_cluster_client_adminResponseCode_UNAUTHORISED_ACCESS,
        msg_text, payload, sizeof(payload));

    aeron_cluster_egress_adapter_on_fragment_for_test(m_adapter, m_buffer, length, nullptr);

    ASSERT_EQ(1u, g_admin_responses.size());
    EXPECT_EQ(cluster_session_id, g_admin_responses[0].cluster_session_id);
    EXPECT_EQ(correlation_id, g_admin_responses[0].correlation_id);
    EXPECT_EQ((int32_t)aeron_cluster_client_adminRequestType_SNAPSHOT, g_admin_responses[0].request_type);
    EXPECT_EQ((int32_t)aeron_cluster_client_adminResponseCode_UNAUTHORISED_ACCESS, g_admin_responses[0].response_code);
    EXPECT_EQ(std::string(msg_text), g_admin_responses[0].message);
    ASSERT_EQ(sizeof(payload), g_admin_responses[0].payload.size());
    EXPECT_EQ(0x1, g_admin_responses[0].payload[0]);
    EXPECT_EQ(0x2, g_admin_responses[0].payload[1]);
    EXPECT_EQ(0x3, g_admin_responses[0].payload[2]);
}

/*
 * Test 8: Admin response dispatches on_admin_response with different session ID.
 * (C adapter does NOT filter admin responses by session ID.)
 */
TEST_F(EgressAdapterTest, shouldDispatchAdminResponseRegardlessOfSessionId)
{
    const int64_t cluster_session_id = 18;
    uint8_t payload[] = {0x1, 0x2, 0x3};
    createAdapter(-cluster_session_id);

    size_t length = encodeAdminResponse(
        cluster_session_id, INT64_C(3274239749237498239),
        aeron_cluster_client_adminRequestType_SNAPSHOT,
        aeron_cluster_client_adminResponseCode_OK,
        "Unauthorised access detected!", payload, sizeof(payload));

    aeron_cluster_egress_adapter_on_fragment_for_test(m_adapter, m_buffer, length, nullptr);

    EXPECT_EQ(1u, g_admin_responses.size());
}

/*
 * Test 9: Unknown schema ID (wrong schemaId in header) causes no dispatch.
 * (Java: defaultEgressListenerBehaviourShouldThrowClusterExceptionOnUnknownSchemaId)
 * In C, the adapter falls through to the default case and does nothing.
 */
TEST_F(EgressAdapterTest, shouldIgnoreUnknownTemplateId)
{
    createAdapter(42);

    /* Manually write a message header with an unknown template ID */
    struct aeron_cluster_client_messageHeader hdr;
    aeron_cluster_client_messageHeader_wrap(
        &hdr, (char *)m_buffer, 0,
        aeron_cluster_client_messageHeader_sbe_schema_version(), sizeof(m_buffer));
    aeron_cluster_client_messageHeader_set_blockLength(&hdr, 8);
    aeron_cluster_client_messageHeader_set_templateId(&hdr, 999);
    aeron_cluster_client_messageHeader_set_schemaId(&hdr, aeron_cluster_client_messageHeader_sbe_schema_id());
    aeron_cluster_client_messageHeader_set_version(&hdr, aeron_cluster_client_messageHeader_sbe_schema_version());

    aeron_cluster_egress_adapter_on_fragment_for_test(m_adapter, m_buffer, 64, nullptr);

    EXPECT_EQ(0u, g_messages.size());
    EXPECT_EQ(0u, g_session_events.size());
    EXPECT_EQ(0u, g_new_leaders.size());
    EXPECT_EQ(0u, g_admin_responses.size());
}

/*
 * Test 10: Fragment too short to contain a message header is silently ignored.
 */
TEST_F(EgressAdapterTest, shouldIgnoreFragmentShorterThanMessageHeader)
{
    createAdapter(42);

    /* Send only 4 bytes -- less than the 8-byte message header */
    memset(m_buffer, 0, 4);

    aeron_cluster_egress_adapter_on_fragment_for_test(m_adapter, m_buffer, 4, nullptr);

    EXPECT_EQ(0u, g_messages.size());
    EXPECT_EQ(0u, g_session_events.size());
    EXPECT_EQ(0u, g_new_leaders.size());
    EXPECT_EQ(0u, g_admin_responses.size());
}
