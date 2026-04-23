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

extern "C"
{
#include "aeron_cluster_client/messageHeader.h"
#include "aeron_cluster_client/sessionEvent.h"
#include "aeron_cluster_client/eventCode.h"
}

/*
 * Port of Java SessionEventCodecCompatibilityTest.readingVersion12UsingVersion5Codec.
 *
 * The Java test encodes a SessionEvent with v1.2 codec and decodes with v0.5 codec,
 * verifying that a newer-version message can be read by an older-version decoder
 * because the schema is backwards-compatible.
 *
 * In C, we only have a single generated codec (the current schema version). The
 * test verifies that a message encoded with the current codec can be decoded back
 * correctly, and confirms the expected schema constants.
 */
TEST(SessionEventCodecCompatibilityTest, shouldEncodeAndDecodeSessionEvent)
{
    uint8_t buffer[1024] = {};

    const int64_t cluster_session_id = -4623823;
    const int64_t correlation_id = INT64_C(3583456348756843);
    const int64_t leadership_term_id = INT64_C(10000000000);
    const int32_t leader_member_id = 2;
    const enum aeron_cluster_client_eventCode code = aeron_cluster_client_eventCode_REDIRECT;
    const int32_t version = (3 << 16) | (47 << 8) | 123;  /* SemanticVersion.compose(3, 47, 123) */
    const char *detail = "some very detailed message";

    /* Encode */
    struct aeron_cluster_client_messageHeader hdr;
    struct aeron_cluster_client_sessionEvent encoder;

    aeron_cluster_client_sessionEvent_wrap_and_apply_header(
        &encoder, (char *)buffer, 0, sizeof(buffer), &hdr);
    aeron_cluster_client_sessionEvent_set_clusterSessionId(&encoder, cluster_session_id);
    aeron_cluster_client_sessionEvent_set_correlationId(&encoder, correlation_id);
    aeron_cluster_client_sessionEvent_set_leadershipTermId(&encoder, leadership_term_id);
    aeron_cluster_client_sessionEvent_set_leaderMemberId(&encoder, leader_member_id);
    aeron_cluster_client_sessionEvent_set_code(&encoder, code);
    aeron_cluster_client_sessionEvent_set_version(&encoder, version);
    aeron_cluster_client_sessionEvent_put_detail(&encoder, detail, (uint32_t)strlen(detail));

    /* Verify schema constants */
    EXPECT_EQ(111, aeron_cluster_client_sessionEvent_sbe_schema_id());
    EXPECT_EQ(2, aeron_cluster_client_sessionEvent_sbe_template_id());

    /* Decode the message header */
    struct aeron_cluster_client_messageHeader hdr_dec;
    aeron_cluster_client_messageHeader_wrap(
        &hdr_dec, (char *)buffer, 0,
        aeron_cluster_client_messageHeader_sbe_schema_version(), sizeof(buffer));

    EXPECT_EQ(aeron_cluster_client_sessionEvent_sbe_schema_id(),
              aeron_cluster_client_messageHeader_schemaId(&hdr_dec));
    EXPECT_EQ(aeron_cluster_client_sessionEvent_sbe_schema_version(),
              aeron_cluster_client_messageHeader_version(&hdr_dec));
    EXPECT_EQ(aeron_cluster_client_sessionEvent_sbe_block_length(),
              aeron_cluster_client_messageHeader_blockLength(&hdr_dec));

    /* Decode with wrap_for_decode, simulating reading with schema info from header */
    struct aeron_cluster_client_sessionEvent decoder;
    uint64_t hdr_len = aeron_cluster_client_messageHeader_encoded_length();
    aeron_cluster_client_sessionEvent_wrap_for_decode(
        &decoder,
        (char *)buffer + hdr_len,
        0,
        aeron_cluster_client_messageHeader_blockLength(&hdr_dec),
        aeron_cluster_client_messageHeader_version(&hdr_dec),
        sizeof(buffer) - hdr_len);

    EXPECT_EQ(cluster_session_id, aeron_cluster_client_sessionEvent_clusterSessionId(&decoder));
    EXPECT_EQ(correlation_id, aeron_cluster_client_sessionEvent_correlationId(&decoder));
    EXPECT_EQ(leadership_term_id, aeron_cluster_client_sessionEvent_leadershipTermId(&decoder));
    EXPECT_EQ(leader_member_id, aeron_cluster_client_sessionEvent_leaderMemberId(&decoder));

    enum aeron_cluster_client_eventCode decoded_code;
    ASSERT_TRUE(aeron_cluster_client_sessionEvent_code(&decoder, &decoded_code));
    EXPECT_EQ(code, decoded_code);

    char detail_buf[256];
    uint32_t detail_len = (uint32_t)aeron_cluster_client_sessionEvent_get_detail(
        &decoder, detail_buf, sizeof(detail_buf) - 1);
    detail_buf[detail_len] = '\0';
    EXPECT_EQ(std::string(detail), std::string(detail_buf));
}
