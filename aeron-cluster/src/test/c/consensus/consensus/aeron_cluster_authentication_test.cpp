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
 * Ported from Java: io.aeron.cluster.AuthenticationTest
 *
 * Tests the pluggable authenticator callbacks on the session manager:
 *   1. shouldAuthenticateOnConnectRequestWithEmptyCredentials
 *   2. shouldAuthenticateOnConnectRequestWithCredentials
 *   3. shouldAuthenticateOnChallengeResponse
 *   4. shouldRejectOnConnectRequest
 *   5. shouldRejectOnChallengeResponse
 *
 * Because these are unit tests (no real Aeron driver), the tests exercise
 * the session manager's on_session_connect → authenticator callback path
 * directly.  Publication connection is not available, so we verify state
 * transitions and pending-list placement rather than full egress flow.
 */

#include <gtest/gtest.h>
#include <cstring>
#include <cstdlib>
#include <cstdint>
#include <string>

extern "C"
{
#include "aeron_cluster_session_manager.h"
#include "aeron_cluster_cluster_session.h"
#include "aeron_common.h"
}

static const char *CREDENTIALS_STRING = "username=\"admin\"|password=\"secret\"";
static const char *CHALLENGE_STRING   = "I challenge you!";
static const char *PRINCIPAL_STRING __attribute__((unused)) = "I am THE Principal!";

/* =======================================================================
 * Test-specific authenticator state shared via clientd.
 * ======================================================================= */
struct auth_test_state_t
{
    int64_t  authenticator_session_id;
    bool     challenge_successful;
    bool     challenge_responded_to;
    bool     on_connect_called;
    bool     on_connected_called;
    bool     on_challenged_called;
    bool     on_challenge_response_called;

    /* Captured credentials from on_connect_request */
    uint8_t  captured_credentials[256];
    size_t   captured_credentials_len;

    /* Captured challenge-response credentials */
    uint8_t  captured_challenge_creds[256];
    size_t   captured_challenge_creds_len;

    /* What the authenticator should do on on_connected_session */
    enum { DO_AUTHENTICATE, DO_CHALLENGE, DO_REJECT } on_connected_action;

    /* What the authenticator should do on on_challenged_session */
    enum { CHALLENGE_AUTHENTICATE, CHALLENGE_REJECT, CHALLENGE_WAIT } on_challenged_action;

    /* Principal to set (may be NULL for empty principal) */
    const uint8_t *principal;
    size_t          principal_len;
};

static void reset_state(auth_test_state_t *s)
{
    memset(s, 0, sizeof(*s));
    s->authenticator_session_id = -1;
    s->on_connected_action      = auth_test_state_t::DO_AUTHENTICATE;
    s->on_challenged_action     = auth_test_state_t::CHALLENGE_WAIT;
}

/* -----------------------------------------------------------------------
 * Authenticator callbacks
 * ----------------------------------------------------------------------- */
static void test_on_connect_request(
    void *clientd, aeron_cluster_cluster_session_t *session, int64_t now_ms)
{
    auto *state = static_cast<auth_test_state_t *>(clientd);
    state->on_connect_called = true;
    state->authenticator_session_id = session->id;

    /* Capture the credentials (stored as encoded_principal at creation) */
    state->captured_credentials_len = session->encoded_principal_length;
    if (session->encoded_principal_length > 0 && session->encoded_principal != nullptr)
    {
        memcpy(state->captured_credentials,
               session->encoded_principal,
               session->encoded_principal_length);
    }
}

static void test_on_connected_session(
    void *clientd, aeron_cluster_cluster_session_t *session, int64_t now_ms)
{
    auto *state = static_cast<auth_test_state_t *>(clientd);
    state->on_connected_called = true;

    switch (state->on_connected_action)
    {
        case auth_test_state_t::DO_AUTHENTICATE:
            aeron_cluster_cluster_session_authenticate(session);
            break;
        case auth_test_state_t::DO_CHALLENGE:
            aeron_cluster_cluster_session_challenge(
                session,
                reinterpret_cast<const uint8_t *>(CHALLENGE_STRING),
                strlen(CHALLENGE_STRING));
            break;
        case auth_test_state_t::DO_REJECT:
            aeron_cluster_cluster_session_reject(session, 1, "Authentication rejected");
            break;
    }
}

static void test_on_challenged_session(
    void *clientd, aeron_cluster_cluster_session_t *session, int64_t now_ms)
{
    auto *state = static_cast<auth_test_state_t *>(clientd);
    state->on_challenged_called = true;

    switch (state->on_challenged_action)
    {
        case auth_test_state_t::CHALLENGE_AUTHENTICATE:
            aeron_cluster_cluster_session_authenticate(session);
            break;
        case auth_test_state_t::CHALLENGE_REJECT:
            aeron_cluster_cluster_session_reject(session, 1, "Challenge rejected");
            break;
        case auth_test_state_t::CHALLENGE_WAIT:
            /* Do nothing — wait for challenge response */
            break;
    }
}

static void test_on_challenge_response(
    void *clientd, int64_t session_id,
    const uint8_t *encoded_credentials, size_t len, int64_t now_ms)
{
    auto *state = static_cast<auth_test_state_t *>(clientd);
    state->on_challenge_response_called = true;

    /* Capture the challenge response credentials */
    state->captured_challenge_creds_len = len;
    if (len > 0 && encoded_credentials != nullptr)
    {
        memcpy(state->captured_challenge_creds, encoded_credentials, len);
    }
}

/* =======================================================================
 * AuthenticationTest fixture
 * ======================================================================= */
class AuthenticationTest : public ::testing::Test
{
protected:
    aeron_cluster_session_manager_t *m_mgr = nullptr;
    auth_test_state_t                m_auth_state = {};

    void SetUp() override
    {
        ASSERT_EQ(0, aeron_cluster_session_manager_create(&m_mgr, 1, nullptr));

        /* Raise the session limit so it doesn't interfere */
        m_mgr->max_concurrent_sessions = 100;

        reset_state(&m_auth_state);

        /* Install custom authenticator */
        m_mgr->authenticator.on_connect_request  = test_on_connect_request;
        m_mgr->authenticator.on_connected_session = test_on_connected_session;
        m_mgr->authenticator.on_challenged_session = test_on_challenged_session;
        m_mgr->authenticator.on_challenge_response = test_on_challenge_response;
        m_mgr->authenticator.clientd               = &m_auth_state;
    }

    void TearDown() override
    {
        aeron_cluster_session_manager_close(m_mgr);
    }

    /*
     * Simulate a session connect as leader with given credentials.
     * version=0 matches AERON_CLUSTER_PROTOCOL_MAJOR_VERSION so no rejection.
     */
    void connectSession(const uint8_t *creds, size_t creds_len,
                        int64_t correlation_id = 42, int64_t now_ns = 1000000000LL)
    {
        aeron_cluster_session_manager_on_session_connect(
            m_mgr,
            correlation_id,
            /* response_stream_id */ 1,
            /* version: major=0, minor=0, patch=0 */ 0,
            "aeron:ipc",
            creds, creds_len,
            now_ns,
            /* is_leader */ true,
            /* ingress_endpoints */ nullptr);
    }

    /*
     * Find the first session in the pending_user list.
     */
    aeron_cluster_cluster_session_t *firstPendingSession()
    {
        if (m_mgr->pending_user_count > 0)
        {
            return m_mgr->pending_user[0];
        }
        return nullptr;
    }
};

/* -----------------------------------------------------------------------
 * Test 1: shouldAuthenticateOnConnectRequestWithEmptyCredentials
 *
 * Java: on_connect_request sees empty credentials, on_connected_session
 * calls authenticate(null), session is authenticated with empty principal.
 * ----------------------------------------------------------------------- */
TEST_F(AuthenticationTest, shouldAuthenticateOnConnectRequestWithEmptyCredentials)
{
    /* Configure: authenticate immediately on connected */
    m_auth_state.on_connected_action = auth_test_state_t::DO_AUTHENTICATE;

    /* Connect with empty (NULL) credentials */
    connectSession(nullptr, 0);

    /* Verify on_connect_request was called */
    EXPECT_TRUE(m_auth_state.on_connect_called);

    /* Session was created and added to pending_user */
    EXPECT_EQ(1, m_mgr->pending_user_count);

    /* Verify the captured credentials were empty */
    EXPECT_EQ(0u, m_auth_state.captured_credentials_len);

    /* The authenticator_session_id should be the assigned session id (1) */
    EXPECT_EQ(1LL, m_auth_state.authenticator_session_id);

    /* The session in pending_user should be INIT (not yet driven by process_pending_sessions)
     * because on_connect_request does NOT call authenticate — that happens in on_connected_session.
     * But on_connect_request IS called from on_session_connect. */
    auto *session = firstPendingSession();
    ASSERT_NE(nullptr, session);
    EXPECT_EQ(1LL, session->id);

    /*
     * Now simulate what process_pending_sessions does:
     * Since there is no real Aeron, the publication won't connect.
     * We manually set the session to CONNECTED state and call
     * on_connected_session to verify the authenticator authenticates it.
     */
    session->state = AERON_CLUSTER_SESSION_STATE_CONNECTED;
    m_mgr->authenticator.on_connected_session(
        m_mgr->authenticator.clientd, session, 1000LL);

    EXPECT_TRUE(m_auth_state.on_connected_called);
    EXPECT_EQ(AERON_CLUSTER_SESSION_STATE_AUTHENTICATED, session->state);

    /* Session had no credentials: encoded_principal should be NULL/length=0 */
    EXPECT_EQ(0u, session->encoded_principal_length);
}

/* -----------------------------------------------------------------------
 * Test 2: shouldAuthenticateOnConnectRequestWithCredentials
 *
 * Java: on_connect_request sees credentials, on_connected_session calls
 * authenticate(PRINCIPAL_STRING.getBytes()), session has principal set.
 * ----------------------------------------------------------------------- */
TEST_F(AuthenticationTest, shouldAuthenticateOnConnectRequestWithCredentials)
{
    m_auth_state.on_connected_action = auth_test_state_t::DO_AUTHENTICATE;

    auto *creds = reinterpret_cast<const uint8_t *>(CREDENTIALS_STRING);
    size_t creds_len = strlen(CREDENTIALS_STRING);

    connectSession(creds, creds_len);

    EXPECT_TRUE(m_auth_state.on_connect_called);
    EXPECT_EQ(1, m_mgr->pending_user_count);

    /* Verify the captured credentials match */
    EXPECT_EQ(creds_len, m_auth_state.captured_credentials_len);
    EXPECT_EQ(0, memcmp(m_auth_state.captured_credentials, CREDENTIALS_STRING, creds_len));

    EXPECT_EQ(1LL, m_auth_state.authenticator_session_id);

    auto *session = firstPendingSession();
    ASSERT_NE(nullptr, session);
    EXPECT_EQ(1LL, session->id);

    /* Simulate the CONNECTED state and call on_connected_session */
    session->state = AERON_CLUSTER_SESSION_STATE_CONNECTED;
    m_mgr->authenticator.on_connected_session(
        m_mgr->authenticator.clientd, session, 1000LL);

    EXPECT_TRUE(m_auth_state.on_connected_called);
    EXPECT_EQ(AERON_CLUSTER_SESSION_STATE_AUTHENTICATED, session->state);

    /* Credentials are stored as encoded_principal at session creation */
    EXPECT_EQ(creds_len, session->encoded_principal_length);
    EXPECT_EQ(0, memcmp(session->encoded_principal, CREDENTIALS_STRING, creds_len));
}

/* -----------------------------------------------------------------------
 * Test 3: shouldAuthenticateOnChallengeResponse
 *
 * Java: on_connect_request sees empty creds, on_connected_session issues
 * a challenge, client responds with credentials, on_challenge_response
 * verifies them, on_challenged_session authenticates with principal.
 * ----------------------------------------------------------------------- */
TEST_F(AuthenticationTest, shouldAuthenticateOnChallengeResponse)
{
    m_auth_state.on_connected_action  = auth_test_state_t::DO_CHALLENGE;
    m_auth_state.on_challenged_action = auth_test_state_t::CHALLENGE_WAIT;

    /* Connect with empty credentials */
    connectSession(nullptr, 0);

    EXPECT_TRUE(m_auth_state.on_connect_called);
    EXPECT_EQ(1, m_mgr->pending_user_count);
    EXPECT_EQ(0u, m_auth_state.captured_credentials_len);

    auto *session = firstPendingSession();
    ASSERT_NE(nullptr, session);
    int64_t session_id = session->id;

    /* Simulate CONNECTED state: on_connected_session issues a challenge */
    session->state = AERON_CLUSTER_SESSION_STATE_CONNECTED;
    m_mgr->authenticator.on_connected_session(
        m_mgr->authenticator.clientd, session, 1000LL);

    EXPECT_TRUE(m_auth_state.on_connected_called);
    EXPECT_EQ(AERON_CLUSTER_SESSION_STATE_CHALLENGED, session->state);
    EXPECT_TRUE(session->has_challenge_pending);
    EXPECT_EQ(strlen(CHALLENGE_STRING), session->encoded_challenge_length);
    EXPECT_EQ(0, memcmp(session->encoded_challenge, CHALLENGE_STRING, strlen(CHALLENGE_STRING)));

    /* Simulate the client sending a challenge response.
     * on_session_manager_on_challenge_response finds the session in pending_user
     * by ID and in CHALLENGED state, then calls the authenticator callback. */
    auto *response_creds = reinterpret_cast<const uint8_t *>(CREDENTIALS_STRING);
    size_t response_len = strlen(CREDENTIALS_STRING);

    aeron_cluster_session_manager_on_challenge_response(
        m_mgr,
        /* correlation_id */ 43,
        session_id,
        response_creds, response_len,
        /* now_ns */ 2000000000LL);

    EXPECT_TRUE(m_auth_state.on_challenge_response_called);
    EXPECT_EQ(response_len, m_auth_state.captured_challenge_creds_len);
    EXPECT_EQ(0, memcmp(m_auth_state.captured_challenge_creds, CREDENTIALS_STRING, response_len));

    /* Now the authenticator has verified the response. Switch action to authenticate
     * so the next on_challenged_session call authenticates the session. */
    m_auth_state.on_challenged_action = auth_test_state_t::CHALLENGE_AUTHENTICATE;

    /* Simulate on_challenged_session tick (called by process_pending_sessions) */
    m_mgr->authenticator.on_challenged_session(
        m_mgr->authenticator.clientd, session, 2000LL);

    EXPECT_TRUE(m_auth_state.on_challenged_called);
    EXPECT_EQ(AERON_CLUSTER_SESSION_STATE_AUTHENTICATED, session->state);
}

/* -----------------------------------------------------------------------
 * Test 4: shouldRejectOnConnectRequest
 *
 * Java: on_connected_session calls sessionProxy.reject(), session should
 * be in REJECTED state.
 * ----------------------------------------------------------------------- */
TEST_F(AuthenticationTest, shouldRejectOnConnectRequest)
{
    m_auth_state.on_connected_action = auth_test_state_t::DO_REJECT;

    connectSession(nullptr, 0);

    EXPECT_TRUE(m_auth_state.on_connect_called);
    EXPECT_EQ(1, m_mgr->pending_user_count);

    auto *session = firstPendingSession();
    ASSERT_NE(nullptr, session);

    /* Simulate CONNECTED → on_connected_session rejects */
    session->state = AERON_CLUSTER_SESSION_STATE_CONNECTED;
    m_mgr->authenticator.on_connected_session(
        m_mgr->authenticator.clientd, session, 1000LL);

    EXPECT_TRUE(m_auth_state.on_connected_called);
    EXPECT_EQ(AERON_CLUSTER_SESSION_STATE_REJECTED, session->state);

    /* Now drive process_pending_sessions — it should move the session
     * from pending_user to rejected_user. */
    int work = aeron_cluster_session_manager_process_pending_sessions(
        m_mgr, /* now_ns */ 1000000000LL, /* now_ms */ 1000LL,
        /* leader_member_id */ 0, /* leadership_term_id */ 1LL);

    EXPECT_GT(work, 0);
    EXPECT_EQ(0, m_mgr->pending_user_count);
    EXPECT_EQ(1, m_mgr->rejected_user_count);

    /* The session is never opened, so active session count stays 0 */
    EXPECT_EQ(0, aeron_cluster_session_manager_session_count(m_mgr));
}

/* -----------------------------------------------------------------------
 * Test 5: shouldRejectOnChallengeResponse
 *
 * Java: on_connected_session challenges, client responds, on_challenge_response
 * records it, on_challenged_session rejects.
 * ----------------------------------------------------------------------- */
TEST_F(AuthenticationTest, shouldRejectOnChallengeResponse)
{
    m_auth_state.on_connected_action  = auth_test_state_t::DO_CHALLENGE;
    m_auth_state.on_challenged_action = auth_test_state_t::CHALLENGE_WAIT;

    connectSession(nullptr, 0);

    EXPECT_TRUE(m_auth_state.on_connect_called);
    EXPECT_EQ(1, m_mgr->pending_user_count);

    auto *session = firstPendingSession();
    ASSERT_NE(nullptr, session);
    int64_t session_id = session->id;

    /* Simulate CONNECTED → challenge issued */
    session->state = AERON_CLUSTER_SESSION_STATE_CONNECTED;
    m_mgr->authenticator.on_connected_session(
        m_mgr->authenticator.clientd, session, 1000LL);

    EXPECT_EQ(AERON_CLUSTER_SESSION_STATE_CHALLENGED, session->state);
    EXPECT_TRUE(session->has_challenge_pending);

    /* Client sends challenge response */
    auto *response_creds = reinterpret_cast<const uint8_t *>(CREDENTIALS_STRING);
    size_t response_len = strlen(CREDENTIALS_STRING);

    aeron_cluster_session_manager_on_challenge_response(
        m_mgr,
        /* correlation_id */ 43,
        session_id,
        response_creds, response_len,
        /* now_ns */ 2000000000LL);

    EXPECT_TRUE(m_auth_state.on_challenge_response_called);
    EXPECT_EQ(response_len, m_auth_state.captured_challenge_creds_len);

    /* After seeing the response, configure authenticator to reject */
    m_auth_state.on_challenged_action = auth_test_state_t::CHALLENGE_REJECT;

    /* Simulate on_challenged_session tick */
    m_mgr->authenticator.on_challenged_session(
        m_mgr->authenticator.clientd, session, 2000LL);

    EXPECT_TRUE(m_auth_state.on_challenged_called);
    EXPECT_EQ(AERON_CLUSTER_SESSION_STATE_REJECTED, session->state);

    /* Drive process_pending_sessions — moves to rejected list */
    int work = aeron_cluster_session_manager_process_pending_sessions(
        m_mgr, /* now_ns */ 2000000000LL, /* now_ms */ 2000LL,
        /* leader_member_id */ 0, /* leadership_term_id */ 1LL);

    EXPECT_GT(work, 0);
    EXPECT_EQ(0, m_mgr->pending_user_count);
    EXPECT_EQ(1, m_mgr->rejected_user_count);
    EXPECT_EQ(0, aeron_cluster_session_manager_session_count(m_mgr));
}
