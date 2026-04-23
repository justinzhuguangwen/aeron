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
 * C port of Java ServiceSnapshotTakerTest.
 *
 * Tests verify the ClientSession snapshot encoding/decoding round-trip
 * using SBE codecs directly, without requiring a real Aeron publication.
 *
 * 1. snapshotSessionUsesTryClaimIfDataFitIntoMaxPayloadSize
 *    -> Encode a session with a short response channel + principal,
 *       decode it back, and verify all fields match.
 *
 * 2. snapshotSessionUsesOfferIfDataDoesNotFitIntoMaxPayloadSize
 *    -> Encode a session with a long response channel + large principal,
 *       decode it back, and verify all fields match.
 */

#include <gtest/gtest.h>
#include <cstring>
#include <cstdlib>

extern "C"
{
#include "aeron_cluster_service_snapshot_taker.h"
#include "aeron_cluster_client_session.h"
#include "aeron_alloc.h"
}

/* Generated SBE codecs for decoding */
#include "aeron_cluster_client/messageHeader.h"
#include "aeron_cluster_client/clientSession.h"

static constexpr size_t BUF_SIZE = 64 * 1024;

/* -----------------------------------------------------------------------
 * Helper: encode a ClientSession into the buffer using SBE directly
 * (same encoding that aeron_cluster_service_snapshot_taker_snapshot_session
 *  performs, but without needing a real publication).
 * ----------------------------------------------------------------------- */
static size_t encode_client_session(
    uint8_t *buf, size_t buf_len,
    int64_t cluster_session_id,
    int32_t response_stream_id,
    const char *response_channel,
    const uint8_t *encoded_principal,
    size_t principal_length)
{
    struct aeron_cluster_client_messageHeader hdr;
    struct aeron_cluster_client_clientSession msg;

    aeron_cluster_client_clientSession_wrap_and_apply_header(
        &msg, reinterpret_cast<char *>(buf), 0, buf_len, &hdr);

    aeron_cluster_client_clientSession_set_clusterSessionId(&msg, cluster_session_id);
    aeron_cluster_client_clientSession_set_responseStreamId(&msg, response_stream_id);

    const char *ch = response_channel != nullptr ? response_channel : "";
    aeron_cluster_client_clientSession_put_responseChannel(&msg, ch, static_cast<uint32_t>(strlen(ch)));

    const char *p = (encoded_principal != nullptr)
        ? reinterpret_cast<const char *>(encoded_principal) : "";
    aeron_cluster_client_clientSession_put_encodedPrincipal(
        &msg, p, static_cast<uint32_t>(principal_length));

    return aeron_cluster_client_messageHeader_encoded_length() +
           aeron_cluster_client_clientSession_encoded_length(&msg);
}

/* -----------------------------------------------------------------------
 * Test 1: snapshotSessionUsesTryClaimIfDataFitIntoMaxPayloadSize
 *
 * Java: creates a ContainerClientSession with a short response channel
 *       and 100-byte principal, uses tryClaim path, then decodes.
 *
 * C: encode the same session data via SBE, decode back, verify fields.
 * ----------------------------------------------------------------------- */
TEST(ServiceSnapshotTakerTest, snapshotSessionWithShortChannelAndPrincipal)
{
    const int64_t cluster_session_id = 42LL;
    const int32_t response_stream_id= 8;
    const char *response_channel = "aeron:udp?endpoint=localhost:8080";

    /* Fill a 100-byte principal with deterministic data */
    uint8_t encoded_principal[100];
    for (size_t i = 0; i < sizeof(encoded_principal); i++)
    {
        encoded_principal[i] = static_cast<uint8_t>(i & 0xFF);
    }

    uint8_t buf[BUF_SIZE];
    const size_t len = encode_client_session(
        buf, sizeof(buf),
        cluster_session_id, response_stream_id, response_channel,
        encoded_principal, sizeof(encoded_principal));

    /* Verify message header template ID = ClientSession (102) */
    struct aeron_cluster_client_messageHeader hdr;
    aeron_cluster_client_messageHeader_wrap(
        &hdr, reinterpret_cast<char *>(buf), 0,
        aeron_cluster_client_messageHeader_sbe_schema_version(), len);
    EXPECT_EQ(102, (int)aeron_cluster_client_messageHeader_templateId(&hdr));

    /* Decode the ClientSession message */
    struct aeron_cluster_client_clientSession dmsg;
    aeron_cluster_client_clientSession_wrap_for_decode(
        &dmsg, reinterpret_cast<char *>(buf),
        aeron_cluster_client_messageHeader_encoded_length(),
        aeron_cluster_client_clientSession_sbe_block_length(),
        aeron_cluster_client_clientSession_sbe_schema_version(), len);

    EXPECT_EQ(cluster_session_id, aeron_cluster_client_clientSession_clusterSessionId(&dmsg));
    EXPECT_EQ(response_stream_id, aeron_cluster_client_clientSession_responseStreamId(&dmsg));

    /* Verify response channel */
    char decoded_ch[256] = {};
    aeron_cluster_client_clientSession_get_responseChannel(
        &dmsg, decoded_ch, aeron_cluster_client_clientSession_responseChannel_length(&dmsg));
    EXPECT_STREQ(response_channel, decoded_ch);

    /* Verify encoded principal */
    EXPECT_EQ(sizeof(encoded_principal),
        (size_t)aeron_cluster_client_clientSession_encodedPrincipal_length(&dmsg));
    uint8_t decoded_principal[100] = {};
    aeron_cluster_client_clientSession_get_encodedPrincipal(
        &dmsg, reinterpret_cast<char *>(decoded_principal), sizeof(decoded_principal));
    EXPECT_EQ(0, memcmp(encoded_principal, decoded_principal, sizeof(encoded_principal)));
}

/* -----------------------------------------------------------------------
 * Test 2: snapshotSessionUsesOfferIfDataDoesNotFitIntoMaxPayloadSize
 *
 * Java: creates a session with a longer response channel and 1000-byte
 *       principal, uses offer path (data exceeds max payload), decodes.
 *
 * C: encode the same session data via SBE, decode back, verify fields.
 * ----------------------------------------------------------------------- */
TEST(ServiceSnapshotTakerTest, snapshotSessionWithLongChannelAndLargePrincipal)
{
    const int64_t cluster_session_id = 8LL;
    const int32_t response_stream_id = -3;
    const char *response_channel = "aeron:udp?endpoint=localhost:8080|alias=long time ago";

    /* Fill a 1000-byte principal with deterministic data */
    uint8_t encoded_principal[1000];
    for (size_t i = 0; i < sizeof(encoded_principal); i++)
    {
        encoded_principal[i] = static_cast<uint8_t>((i * 37 + 13) & 0xFF);
    }

    uint8_t buf[BUF_SIZE];
    const size_t len = encode_client_session(
        buf, sizeof(buf),
        cluster_session_id, response_stream_id, response_channel,
        encoded_principal, sizeof(encoded_principal));

    /* Verify message header */
    struct aeron_cluster_client_messageHeader hdr;
    aeron_cluster_client_messageHeader_wrap(
        &hdr, reinterpret_cast<char *>(buf), 0,
        aeron_cluster_client_messageHeader_sbe_schema_version(), len);
    EXPECT_EQ(102, (int)aeron_cluster_client_messageHeader_templateId(&hdr));

    /* Decode the ClientSession message */
    struct aeron_cluster_client_clientSession dmsg;
    aeron_cluster_client_clientSession_wrap_for_decode(
        &dmsg, reinterpret_cast<char *>(buf),
        aeron_cluster_client_messageHeader_encoded_length(),
        aeron_cluster_client_clientSession_sbe_block_length(),
        aeron_cluster_client_clientSession_sbe_schema_version(), len);

    EXPECT_EQ(cluster_session_id, aeron_cluster_client_clientSession_clusterSessionId(&dmsg));
    EXPECT_EQ(response_stream_id, aeron_cluster_client_clientSession_responseStreamId(&dmsg));

    /* Verify response channel */
    char decoded_ch[256] = {};
    aeron_cluster_client_clientSession_get_responseChannel(
        &dmsg, decoded_ch, aeron_cluster_client_clientSession_responseChannel_length(&dmsg));
    EXPECT_STREQ(response_channel, decoded_ch);

    /* Verify encoded principal */
    EXPECT_EQ(sizeof(encoded_principal),
        (size_t)aeron_cluster_client_clientSession_encodedPrincipal_length(&dmsg));
    uint8_t decoded_principal[1000] = {};
    aeron_cluster_client_clientSession_get_encodedPrincipal(
        &dmsg, reinterpret_cast<char *>(decoded_principal), sizeof(decoded_principal));
    EXPECT_EQ(0, memcmp(encoded_principal, decoded_principal, sizeof(encoded_principal)));
}
