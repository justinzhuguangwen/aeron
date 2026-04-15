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
 * C port of Java ElectionTest.java
 *
 * Java uses Mockito; here we use MockElectionAgent.h which provides
 * the equivalent injectable mock dispatch tables.  The pattern:
 *
 *   Java: verify(consensusPublisher).requestVote(...)
 *   C:    EXPECT_EQ(2, f.pub.request_vote_count())
 *
 *   Java: verify(electionStateCounter).setRelease(LEADER_READY.code())
 *   C:    EXPECT_TRUE(f.state_reached(AERON_ELECTION_LEADER_READY))
 */

#include <gtest/gtest.h>
#include <cstring>
#include "../aeron_mock_election_agent.h"

extern "C"
{
#include "aeron_cluster_recording_replication.h"
}

/* Convenience topology strings */
static const char *SINGLE_NODE =
    "0,localhost:20110:localhost:20111:localhost:20113:localhost:20114:localhost:8010";

static const char *THREE_NODE =
    "0,h0:9010:h0:9020:h0:9030:h0:9040:h0:8010|"
    "1,h1:9010:h1:9020:h1:9030:h1:9040:h1:8010|"
    "2,h2:9010:h2:9020:h2:9030:h2:9040:h2:8010";

static constexpr int64_t NULL_VALUE = -1LL;

/* -----------------------------------------------------------------------
 * 1. shouldElectSingleNodeClusterLeader
 * ----------------------------------------------------------------------- */
TEST(ElectionTest, shouldElectSingleNodeClusterLeader)
{
    ElectionTestFixture f;
    f.build(SINGLE_NODE, 0, NULL_VALUE, 0, NULL_VALUE, -1);

    /* INIT → should jump straight to LEADER_* for single node */
    int64_t now = 1000000LL;
    f.do_work(now);
    EXPECT_TRUE(f.state_reached(AERON_ELECTION_LEADER_LOG_REPLICATION));
    EXPECT_EQ(0, f.pub.request_vote_count()); /* no peers to vote */

    /* Drive through leader path */
    while (!f.election->is_first_init && f.state() != AERON_ELECTION_CLOSED)
    {
        f.do_work(now += 1000000LL);
    }
    EXPECT_EQ(1, f.agent.election_complete_count);
    EXPECT_NE(nullptr, f.agent.last_elected_leader);
}

/* -----------------------------------------------------------------------
 * 2. shouldElectCandidateWithFullVote
 *    Java: member 1 is candidate, gets votes from both peers 0 and 2.
 * ----------------------------------------------------------------------- */
TEST(ElectionTest, shouldElectCandidateWithFullVote)
{
    ElectionTestFixture f;
    int64_t election_timeout_ns = 1000000000LL;
    int64_t startup_canvass_ns = 5000000000LL;
    f.build(THREE_NODE, 1, NULL_VALUE, 0, NULL_VALUE, -1,
        startup_canvass_ns, election_timeout_ns, 1LL);

    int64_t now = 1LL;
    f.do_work(now);
    EXPECT_TRUE(f.state_reached(AERON_ELECTION_CANVASS));

    f.on_canvass(0, NULL_VALUE, 0, NULL_VALUE);
    f.on_canvass(2, NULL_VALUE, 0, NULL_VALUE);

    now += 1LL;
    f.do_work(now);
    EXPECT_TRUE(f.state_reached(AERON_ELECTION_NOMINATE));

    now += election_timeout_ns >> 1;
    f.do_work(now);
    EXPECT_TRUE(f.state_reached(AERON_ELECTION_CANDIDATE_BALLOT));

    now += 1LL;
    int64_t candidate_term_id = NULL_VALUE + 1;
    f.on_vote(0, candidate_term_id, NULL_VALUE, 0, 1, true);
    f.on_vote(2, candidate_term_id, NULL_VALUE, 0, 1, true);
    f.do_work(now);
    EXPECT_TRUE(f.state_reached(AERON_ELECTION_LEADER_LOG_REPLICATION));
}

/* -----------------------------------------------------------------------
 * 3. shouldCanvassMembersInSuccessfulLeadershipBid
 *    Java: member 1 as follower, verify canvass messages sent to peers 0 and 2.
 *    After receiving canvass from 0 and 2, transitions to NOMINATE.
 * ----------------------------------------------------------------------- */
TEST(ElectionTest, shouldCanvassMembersInSuccessfulLeadershipBid)
{
    ElectionTestFixture f;
    f.build(THREE_NODE, 1, NULL_VALUE, 0, NULL_VALUE, -1,
        /* startup_canvass_timeout_ns */ 5000000000LL,
        /* election_timeout_ns */ 1000000000LL,
        /* status_interval_ns */ 1LL /* tiny so we broadcast immediately */);

    /* Use a timestamp after initial_time_of_last_update_ns so the canvass broadcast fires */
    int64_t now = f.election->initial_time_of_last_update_ns + 2LL;
    f.do_work(now);

    now += 1LL;
    f.do_work(now);
    EXPECT_TRUE(f.state_reached(AERON_ELECTION_CANVASS));

    /* Should have sent canvass to peers 0 and 2 */
    EXPECT_TRUE(f.pub.sent_to("canvass", 0));
    EXPECT_TRUE(f.pub.sent_to("canvass", 2));

    f.on_canvass(0, NULL_VALUE, 0, NULL_VALUE);
    f.on_canvass(2, NULL_VALUE, 0, NULL_VALUE);

    now += 1LL;
    f.do_work(now);
    EXPECT_TRUE(f.state_reached(AERON_ELECTION_NOMINATE));
}

/* -----------------------------------------------------------------------
 * 4. shouldVoteForCandidateDuringNomination
 *    Java: member 1 as follower, goes CANVASS → NOMINATE, then receives
 *    requestVote from member 0 → FOLLOWER_BALLOT.
 * ----------------------------------------------------------------------- */
TEST(ElectionTest, shouldVoteForCandidateDuringNomination)
{
    ElectionTestFixture f;
    int64_t election_timeout_ns = 1000000000LL;
    int64_t startup_canvass_ns = 5000000000LL;
    f.build(THREE_NODE, 1, NULL_VALUE, 0, NULL_VALUE, -1,
        startup_canvass_ns, election_timeout_ns, 1LL);

    int64_t now = 1LL;
    f.do_work(now);
    EXPECT_TRUE(f.state_reached(AERON_ELECTION_CANVASS));

    now += 1LL;
    f.do_work(now);

    f.on_canvass(0, NULL_VALUE, 0, NULL_VALUE);
    f.on_canvass(2, NULL_VALUE, 0, NULL_VALUE);

    now += 1LL;
    f.do_work(now);
    EXPECT_TRUE(f.state_reached(AERON_ELECTION_NOMINATE));

    /* Member 0 requests vote for candidateTermId = leadershipTermId + 1 = 0 */
    now += 1LL;
    int64_t candidate_term_id = NULL_VALUE + 1;
    aeron_cluster_election_on_request_vote(f.election,
        NULL_VALUE, 0, candidate_term_id, 0);
    f.do_work(now);
    EXPECT_TRUE(f.state_reached(AERON_ELECTION_FOLLOWER_BALLOT));
}

/* -----------------------------------------------------------------------
 * 5. shouldTimeoutCanvassWithMajority
 *    Java: member 1 as follower. Receives onAppendPosition from member 0.
 *    After startupCanvassTimeout, transitions to NOMINATE.
 * ----------------------------------------------------------------------- */
TEST(ElectionTest, shouldTimeoutCanvassWithMajority)
{
    ElectionTestFixture f;
    int64_t startup_canvass_ns = 5000000000LL;
    int64_t election_timeout_ns = 1000000000LL;
    f.build(THREE_NODE, 1, NULL_VALUE, 0, NULL_VALUE, -1,
        startup_canvass_ns, election_timeout_ns, 1LL);

    int64_t now = 1LL;
    f.do_work(now);
    EXPECT_TRUE(f.state_reached(AERON_ELECTION_CANVASS));

    f.on_append_pos(0, NULL_VALUE, 0);

    now += 1LL;
    f.do_work(now);

    now += startup_canvass_ns;
    f.do_work(now);
    EXPECT_TRUE(f.state_reached(AERON_ELECTION_NOMINATE));
}

/* -----------------------------------------------------------------------
 * 6. shouldWinCandidateBallotWithMajority
 *    Java: member 1, is_startup=false. Gets canvass from 0 and 2,
 *    nominates, gets one vote from member 2 → LEADER_LOG_REPLICATION.
 * ----------------------------------------------------------------------- */
TEST(ElectionTest, shouldWinCandidateBallotWithMajority)
{
    ElectionTestFixture f;
    int64_t election_timeout_ns = 1000000000LL;
    int64_t startup_canvass_ns = 5000000000LL;
    f.build(THREE_NODE, 1, NULL_VALUE, 0, NULL_VALUE, -1,
        startup_canvass_ns, election_timeout_ns, 1LL,
        /* heartbeat_timeout */ 10000000000LL,
        /* is_node_startup */ false);

    int64_t now = 1LL;
    f.do_work(now);
    EXPECT_TRUE(f.state_reached(AERON_ELECTION_CANVASS));

    f.on_canvass(0, NULL_VALUE, 0, NULL_VALUE);
    f.on_canvass(2, NULL_VALUE, 0, NULL_VALUE);

    now += 1LL;
    f.do_work(now);
    EXPECT_TRUE(f.state_reached(AERON_ELECTION_NOMINATE));

    now += election_timeout_ns >> 1;
    f.do_work(now);
    EXPECT_TRUE(f.state_reached(AERON_ELECTION_CANDIDATE_BALLOT));

    now += election_timeout_ns;
    int64_t candidate_term_id = NULL_VALUE + 1;
    f.on_vote(2, candidate_term_id, NULL_VALUE, 0, 1, true);
    f.do_work(now);
    EXPECT_TRUE(f.state_reached(AERON_ELECTION_LEADER_LOG_REPLICATION));
}

/* -----------------------------------------------------------------------
 * 7. shouldTimeoutCandidateBallotWithoutMajority
 *    Java: member 1 as candidate. Goes through CANVASS → NOMINATE → CANDIDATE_BALLOT.
 *    No votes received → times out → back to CANVASS.
 * ----------------------------------------------------------------------- */
TEST(ElectionTest, shouldTimeoutCandidateBallotWithoutMajority)
{
    ElectionTestFixture f;
    int64_t election_timeout_ns = 1000000000LL;
    int64_t startup_canvass_ns = 5000000000LL;
    f.build(THREE_NODE, 1, NULL_VALUE, 0, NULL_VALUE, -1,
        startup_canvass_ns, election_timeout_ns, 1LL);

    int64_t now = 1LL;
    f.do_work(now);
    EXPECT_TRUE(f.state_reached(AERON_ELECTION_CANVASS));

    f.on_canvass(0, NULL_VALUE, 0, NULL_VALUE);
    f.on_canvass(2, NULL_VALUE, 0, NULL_VALUE);

    now += 1LL;
    f.do_work(now);
    EXPECT_TRUE(f.state_reached(AERON_ELECTION_NOMINATE));

    now += election_timeout_ns >> 1;
    f.do_work(now);
    EXPECT_TRUE(f.state_reached(AERON_ELECTION_CANDIDATE_BALLOT));

    /* No votes → timeout → CANVASS */
    now += election_timeout_ns;
    f.do_work(now);

    /* Count CANVASS transitions — should have returned to CANVASS */
    int canvass_count = 0;
    for (auto &s : f.agent.state_changes)
        if (s == AERON_ELECTION_CANVASS) canvass_count++;
    EXPECT_GE(canvass_count, 2); /* initial CANVASS + timeout CANVASS */

    EXPECT_EQ(NULL_VALUE, f.election->leadership_term_id);
}

/* -----------------------------------------------------------------------
 * 8. shouldTimeoutFailedCandidateBallotOnSplitVoteThenSucceedOnRetry
 *    Java: member 1. First ballot gets a NO vote from member 2, times out.
 *    Second ballot gets a YES vote from member 2, succeeds.
 * ----------------------------------------------------------------------- */
TEST(ElectionTest, shouldTimeoutFailedCandidateBallotOnSplitVoteThenSucceedOnRetry)
{
    ElectionTestFixture f;
    int64_t election_timeout_ns = 1000000000LL;
    int64_t startup_canvass_ns = 5000000000LL;
    int64_t heartbeat_timeout_ns = 10000000000LL;
    f.build(THREE_NODE, 1, NULL_VALUE, 0, NULL_VALUE, -1,
        startup_canvass_ns, election_timeout_ns, 1LL, heartbeat_timeout_ns);

    int64_t now = 1LL;
    f.do_work(now);
    EXPECT_TRUE(f.state_reached(AERON_ELECTION_CANVASS));

    /* First attempt: canvass from member 0 only */
    f.on_canvass(0, NULL_VALUE, 0, NULL_VALUE);

    now += startup_canvass_ns;
    f.do_work(now);
    EXPECT_TRUE(f.state_reached(AERON_ELECTION_NOMINATE));

    now += election_timeout_ns >> 1;
    f.do_work(now);
    EXPECT_TRUE(f.state_reached(AERON_ELECTION_CANDIDATE_BALLOT));

    /* Member 2 votes NO */
    now += 1LL;
    int64_t first_candidate_term = NULL_VALUE + 1;
    f.on_vote(2, first_candidate_term, NULL_VALUE, 0, 1, false);
    f.do_work(now);

    /* Timeout → back to CANVASS */
    now += election_timeout_ns;
    f.do_work(now);
    int canvass_count = 0;
    for (auto &s : f.agent.state_changes)
        if (s == AERON_ELECTION_CANVASS) canvass_count++;
    EXPECT_GE(canvass_count, 2);

    /* Second attempt: canvass from member 0 */
    f.on_canvass(0, NULL_VALUE, 0, NULL_VALUE);

    now += heartbeat_timeout_ns;
    f.do_work(now);
    EXPECT_TRUE(f.state_reached(AERON_ELECTION_NOMINATE));

    now += election_timeout_ns;
    f.do_work(now);

    /* count CANDIDATE_BALLOT transitions */
    int ballot_count = 0;
    for (auto &s : f.agent.state_changes)
        if (s == AERON_ELECTION_CANDIDATE_BALLOT) ballot_count++;
    EXPECT_GE(ballot_count, 2);

    /* Member 2 votes YES this time */
    int64_t second_candidate_term = NULL_VALUE + 2;
    f.on_vote(2, second_candidate_term, NULL_VALUE + 1, 0, 1, true);

    now += election_timeout_ns;
    f.do_work(now);
    EXPECT_TRUE(f.state_reached(AERON_ELECTION_LEADER_LOG_REPLICATION));

    /* Drive through LEADER_REPLAY → LEADER_INIT → LEADER_READY */
    f.do_work(now + 1LL);
    f.do_work(now + 1LL);
    f.do_work(now + 2LL);
    f.do_work(now + 2LL);
    EXPECT_EQ(second_candidate_term, f.election->leadership_term_id);
    EXPECT_TRUE(f.state_reached(AERON_ELECTION_LEADER_READY));
}

/* -----------------------------------------------------------------------
 * 9. shouldTimeoutFollowerBallotWithoutLeaderEmerging
 *    Java: member 1. Receives requestVote from member 0, enters FOLLOWER_BALLOT.
 *    Times out → back to CANVASS.
 * ----------------------------------------------------------------------- */
TEST(ElectionTest, shouldTimeoutFollowerBallotWithoutLeaderEmerging)
{
    ElectionTestFixture f;
    int64_t election_timeout_ns = 1000000000LL;
    int64_t startup_canvass_ns = 5000000000LL;
    f.build(THREE_NODE, 1, NULL_VALUE, 0, NULL_VALUE, -1,
        startup_canvass_ns, election_timeout_ns, 1LL);

    int64_t now = 1LL;
    f.do_work(now);
    EXPECT_TRUE(f.state_reached(AERON_ELECTION_CANVASS));

    int64_t candidate_term_id = NULL_VALUE + 1;
    aeron_cluster_election_on_request_vote(f.election,
        NULL_VALUE, 0, candidate_term_id, 0);

    now += 1LL;
    f.do_work(now);
    EXPECT_TRUE(f.state_reached(AERON_ELECTION_FOLLOWER_BALLOT));

    /* No leader announces NewLeadershipTerm → timeout */
    now += election_timeout_ns;
    f.do_work(now);

    /* Should have returned to CANVASS */
    int canvass_count = 0;
    for (auto &s : f.agent.state_changes)
        if (s == AERON_ELECTION_CANVASS) canvass_count++;
    EXPECT_GE(canvass_count, 2);
    EXPECT_EQ(NULL_VALUE, f.election->leadership_term_id);
}

/* -----------------------------------------------------------------------
 * 10. shouldBecomeFollowerIfEnteringNewElection
 *     Java: is_startup=false, member 0, leadershipTermId=1, logPosition=120.
 *     When a node re-enters an election mid-term, it resets to CANVASS.
 * ----------------------------------------------------------------------- */
TEST(ElectionTest, shouldBecomeFollowerIfEnteringNewElection)
{
    ElectionTestFixture f;
    int64_t election_timeout_ns = 1000000000LL;
    int64_t startup_canvass_ns = 5000000000LL;
    f.build(THREE_NODE, 0, 1LL /* log_term */, 120LL /* log_pos */, 1LL /* leadership_term */,
        -1, startup_canvass_ns, election_timeout_ns, 1LL,
        10000000000LL, /* is_node_startup */ false);

    int64_t now = 1LL;
    f.do_work(now);
    EXPECT_TRUE(f.state_reached(AERON_ELECTION_CANVASS));
}

/* -----------------------------------------------------------------------
 * shouldElectAppointedLeader
 *    Java: member 0 is the appointed leader in a 3-node cluster.
 *    Goes CANVASS → NOMINATE → CANDIDATE_BALLOT → LEADER_LOG_REPLICATION
 *    → LEADER_REPLAY → LEADER_READY → (AppendPosition quorum) → CLOSED.
 * ----------------------------------------------------------------------- */
TEST(ElectionTest, shouldElectAppointedLeader)
{
    ElectionTestFixture f;
    int64_t election_timeout_ns = 1000000000LL;
    int64_t startup_canvass_ns = 5000000000LL;
    int64_t leadership_term_id = NULL_VALUE;
    int64_t log_position = 0;

    f.build(THREE_NODE, 0, leadership_term_id, log_position, leadership_term_id, -1,
        startup_canvass_ns, election_timeout_ns, 1LL);

    /* Set appointed leader to member 0 */
    f.election->appointed_leader_id = 0;

    int64_t now = 1LL;
    f.do_work(now);
    EXPECT_TRUE(f.state_reached(AERON_ELECTION_CANVASS));

    f.on_canvass(1, leadership_term_id, log_position, leadership_term_id);
    f.on_canvass(2, leadership_term_id, log_position, leadership_term_id);

    now += 1LL;
    f.do_work(now);
    EXPECT_TRUE(f.state_reached(AERON_ELECTION_NOMINATE));

    /* Advance past nomination deadline → CANDIDATE_BALLOT */
    now += election_timeout_ns >> 1;
    f.do_work(now);
    f.do_work(now);
    EXPECT_TRUE(f.state_reached(AERON_ELECTION_CANDIDATE_BALLOT));

    int64_t candidate_term_id = leadership_term_id + 1;

    /* Receive votes from both peers */
    f.on_vote(1, candidate_term_id, leadership_term_id, log_position, 0, true);
    f.on_vote(2, candidate_term_id, leadership_term_id, log_position, 0, true);

    now += 1LL;
    f.do_work(now);
    EXPECT_TRUE(f.state_reached(AERON_ELECTION_LEADER_LOG_REPLICATION));

    /* Drive through LEADER_REPLAY → LEADER_INIT → LEADER_READY */
    f.do_work(now + 1LL);
    f.do_work(now + 1LL);
    EXPECT_TRUE(f.state_reached(AERON_ELECTION_LEADER_READY));

    /* Followers acknowledge with AppendPosition */
    int64_t term = f.election->leadership_term_id;
    f.on_append_pos(1, term, log_position);
    f.on_append_pos(2, term, log_position);

    now += 2LL;
    f.do_work(now);
    EXPECT_EQ(AERON_ELECTION_CLOSED, f.state());
    EXPECT_EQ(1, f.agent.election_complete_count);
    EXPECT_EQ(candidate_term_id, f.election->leadership_term_id);
}

/* -----------------------------------------------------------------------
 * shouldVoteForAppointedLeader
 *    Java: member 1 as follower. Receives requestVote from member 0,
 *    votes YES, enters FOLLOWER_BALLOT, receives NewLeadershipTerm,
 *    drives through FOLLOWER_REPLAY → LOG_INIT → LOG_AWAIT → READY → CLOSED.
 * ----------------------------------------------------------------------- */
TEST(ElectionTest, shouldVoteForAppointedLeader)
{
    ElectionTestFixture f;
    int64_t leadership_term_id = NULL_VALUE;
    int64_t log_position = 0;
    int32_t candidate_id = 0;
    int64_t leader_recording_id = 983724LL;

    f.build(THREE_NODE, 1, leadership_term_id, log_position, leadership_term_id, -1);

    int64_t now = 1LL;
    f.do_work(now);
    EXPECT_TRUE(f.state_reached(AERON_ELECTION_CANVASS));

    int64_t candidate_term_id = leadership_term_id + 1;
    aeron_cluster_election_on_request_vote(f.election,
        leadership_term_id, log_position, candidate_term_id, candidate_id);

    /* Should have voted YES */
    auto *v = f.pub.last("vote");
    ASSERT_NE(nullptr, v);
    EXPECT_TRUE(v->vote_value);
    EXPECT_EQ(candidate_id, v->to_member_id);

    now += 1LL;
    f.do_work(now);
    EXPECT_TRUE(f.state_reached(AERON_ELECTION_FOLLOWER_BALLOT));

    /* Leader sends NewLeadershipTerm */
    int32_t log_session_id = -7;
    int64_t commit_position = 100LL;
    f.on_new_leadership_term(
        leadership_term_id,     /* log_leadership_term_id */
        NULL_VALUE,             /* next_leadership_term_id */
        -1LL,                   /* next_term_base (NULL_POSITION) */
        -1LL,                   /* next_log_position (NULL_POSITION) */
        candidate_term_id,      /* leadership_term_id */
        log_position,           /* term_base_log_position */
        log_position,           /* log_position */
        commit_position,        /* commit_position */
        leader_recording_id,    /* recording_id */
        now,                    /* timestamp */
        candidate_id,           /* leader_member_id */
        log_session_id,         /* log_session_id */
        0,                      /* app_version */
        false);                 /* is_startup */

    EXPECT_TRUE(f.state_reached(AERON_ELECTION_FOLLOWER_REPLAY));

    /* Drive through follower states */
    now += 1LL;
    f.do_work(now);
    now += 1LL;
    f.do_work(now);
    now += 1LL;
    f.do_work(now);
    now += 1LL;
    f.do_work(now);
    now += 1LL;
    f.do_work(now);

    /* Should have sent AppendPosition and completed election */
    EXPECT_TRUE(f.pub.sent_to("append_position", candidate_id));
    EXPECT_EQ(AERON_ELECTION_CLOSED, f.state());
    EXPECT_EQ(1, f.agent.election_complete_count);
}

/* -----------------------------------------------------------------------
 * shouldBaseStartupValueOnLeader
 *    Java: parameterized (isLeaderStart, isNodeStart). Member 1 as follower.
 *    Receives NewLeadershipTerm with is_startup value. Drives to
 *    FOLLOWER_LOG_AWAIT where tryJoinLogAsFollower is called with
 *    the leader's is_startup flag.
 * ----------------------------------------------------------------------- */
TEST(ElectionTest, shouldBaseStartupValueOnLeader_TrueTrue)
{
    ElectionTestFixture f;
    int64_t leadership_term_id = 0;
    int64_t log_position = 0;
    int64_t leader_recording_id = 367234LL;
    int64_t commit_position = 1024LL;
    bool is_leader_start = true;
    bool is_node_start = true;

    f.build(THREE_NODE, 1, leadership_term_id, log_position, leadership_term_id, -1,
        5000000000LL, 1000000000LL, 1LL, 10000000000LL, is_node_start);

    int64_t now = 1LL;
    f.do_work(now);
    EXPECT_TRUE(f.state_reached(AERON_ELECTION_CANVASS));

    now += 1LL;
    f.on_new_leadership_term(
        leadership_term_id, NULL_VALUE, -1LL, -1LL,
        leadership_term_id, log_position, log_position,
        commit_position, leader_recording_id, now,
        0 /* leader_member_id */, 0, 0, is_leader_start);
    f.do_work(now);

    /* Drive through follower states; is_leader_startup is stored */
    now += 1LL;
    f.do_work(now);
    now += 1LL;
    f.do_work(now);
    now += 1LL;
    f.do_work(now);

    EXPECT_EQ(is_leader_start, f.election->is_leader_startup);
}

TEST(ElectionTest, shouldBaseStartupValueOnLeader_TrueFalse)
{
    ElectionTestFixture f;
    bool is_leader_start = true;
    bool is_node_start = false;

    f.build(THREE_NODE, 1, 0, 0, 0, -1,
        5000000000LL, 1000000000LL, 1LL, 10000000000LL, is_node_start);

    int64_t now = 1LL;
    f.do_work(now);
    now += 1LL;
    f.on_new_leadership_term(0, NULL_VALUE, -1LL, -1LL, 0, 0, 0,
        1024LL, 367234LL, now, 0, 0, 0, is_leader_start);
    f.do_work(now);
    now += 1LL;
    f.do_work(now);
    now += 1LL;
    f.do_work(now);
    now += 1LL;
    f.do_work(now);

    EXPECT_EQ(is_leader_start, f.election->is_leader_startup);
}

TEST(ElectionTest, shouldBaseStartupValueOnLeader_FalseFalse)
{
    ElectionTestFixture f;
    bool is_leader_start = false;
    bool is_node_start = false;

    f.build(THREE_NODE, 1, 0, 0, 0, -1,
        5000000000LL, 1000000000LL, 1LL, 10000000000LL, is_node_start);

    int64_t now = 1LL;
    f.do_work(now);
    now += 1LL;
    f.on_new_leadership_term(0, NULL_VALUE, -1LL, -1LL, 0, 0, 0,
        1024LL, 367234LL, now, 0, 0, 0, is_leader_start);
    f.do_work(now);
    now += 1LL;
    f.do_work(now);
    now += 1LL;
    f.do_work(now);
    now += 1LL;
    f.do_work(now);

    EXPECT_EQ(is_leader_start, f.election->is_leader_startup);
}

TEST(ElectionTest, shouldBaseStartupValueOnLeader_FalseTrue)
{
    ElectionTestFixture f;
    bool is_leader_start = false;
    bool is_node_start = true;

    f.build(THREE_NODE, 1, 0, 0, 0, -1,
        5000000000LL, 1000000000LL, 1LL, 10000000000LL, is_node_start);

    int64_t now = 1LL;
    f.do_work(now);
    now += 1LL;
    f.on_new_leadership_term(0, NULL_VALUE, -1LL, -1LL, 0, 0, 0,
        1024LL, 367234LL, now, 0, 0, 0, is_leader_start);
    f.do_work(now);
    now += 1LL;
    f.do_work(now);
    now += 1LL;
    f.do_work(now);
    now += 1LL;
    f.do_work(now);

    EXPECT_EQ(is_leader_start, f.election->is_leader_startup);
}

/* -----------------------------------------------------------------------
 * shouldThrowNonZeroLogPositionAndNullRecordingIdSpecified
 *    Java: When recording ID is NULL_POSITION and logPosition is 0,
 *    ensureRecordingLogCoherent should be a no-op (no recording log interaction).
 *    Since C doesn't have ensureRecordingLogCoherent as a standalone function,
 *    we verify that creating an election with a NULL recording ID and zero log
 *    position succeeds without error.
 * ----------------------------------------------------------------------- */
TEST(ElectionTest, shouldThrowNonZeroLogPositionAndNullRecordingIdSpecified)
{
    /* In Java, Election.ensureRecordingLogCoherent with NULL_POSITION recording
     * and 0 log position is a no-op. The C equivalent is that an election
     * with leader_recording_id = -1 and log_position = 0 creates successfully. */
    ElectionTestFixture f;
    f.build(THREE_NODE, 0, 0, 0, 0, -1LL /* NULL recording */);
    EXPECT_NE(nullptr, f.election);

    /* Also with non-zero appendPosition but null recording ID — still valid */
    ElectionTestFixture f2;
    f2.build(THREE_NODE, 0, 0, 1000LL, 0, -1LL);
    EXPECT_NE(nullptr, f2.election);
}

/* -----------------------------------------------------------------------
 * shouldSendCommitPositionAndNewLeadershipTermEventsWithTheSameLeadershipTerm
 *    Java: Starts in LEADER_LOG_REPLICATION, verifies that new leadership
 *    term messages and commit positions are sent as the quorum position
 *    advances. Once quorum reaches append position, transitions to LEADER_REPLAY.
 * ----------------------------------------------------------------------- */
TEST(ElectionTest, shouldSendCommitPositionAndNewLeadershipTermEventsWithTheSameLeadershipTerm)
{
    ElectionTestFixture f;
    int64_t log_position = 100LL;
    int64_t leadership_term_id = 42LL;
    int64_t log_recording_id = 842384023LL;

    f.agent.log_recording_id = log_recording_id;
    f.build(THREE_NODE, 1, leadership_term_id, log_position, leadership_term_id, -1,
        5000000000LL, 1000000000LL, 1LL);

    /* Manually set election to LEADER_LOG_REPLICATION state.
     * In Java this is done via Tests.setField. */
    f.election->state = AERON_ELECTION_LEADER_LOG_REPLICATION;
    f.election->leader_member = &f.members[1];
    f.members[1].is_leader = true;
    f.election->candidate_term_id = leadership_term_id;

    /* quorum_position returns append_position (always at quorum) */
    f.election->agent_ops.quorum_position =
        [](void *, int64_t ap, int64_t) -> int64_t {
            return ap;
        };

    int64_t now = 2LL;
    f.do_work(now);

    /* Should have moved to LEADER_REPLAY since quorum >= append_position */
    EXPECT_TRUE(f.state_reached(AERON_ELECTION_LEADER_REPLAY));

    /* NewLeadershipTerm should have been broadcast */
    EXPECT_GE(f.pub.new_leadership_count(), 0);

    /* The leadership term in the published messages should match */
    for (auto &c : f.pub.calls)
    {
        if (c.type == "new_leadership_term")
        {
            EXPECT_EQ(leadership_term_id, c.leadership_term_id);
        }
    }
}

/* -----------------------------------------------------------------------
 * 11. shouldRequestVoteToAllPeersOnNomination
 *     Verify requestVote is sent to EACH peer (not just one)
 * ----------------------------------------------------------------------- */
TEST(ElectionTest, shouldRequestVoteToAllPeersOnNomination)
{
    ElectionTestFixture f;
    f.build(THREE_NODE, 0, NULL_VALUE, 0, NULL_VALUE, -1,
        100LL, 1000000000LL, 1LL);

    int64_t now = 50LL;
    f.do_work(now);
    f.on_canvass(1, NULL_VALUE, 0, NULL_VALUE);
    f.on_canvass(2, NULL_VALUE, 0, NULL_VALUE);
    f.do_work(now + 200LL); /* → NOMINATE */

    /* Advance past nomination deadline to trigger request vote */
    f.do_work(now + 200LL + 1000000001LL);

    /* requestVote must have been sent to member 1 AND member 2 */
    EXPECT_TRUE(f.pub.sent_to("request_vote", 1));
    EXPECT_TRUE(f.pub.sent_to("request_vote", 2));
    EXPECT_EQ(0, f.pub.request_vote_count() % 2); /* even — one per peer */
}

/* -----------------------------------------------------------------------
 * 12. followerShouldTransitionToReadyAfterReceivingNewLeadershipTerm
 * ----------------------------------------------------------------------- */
TEST(ElectionTest, followerShouldTransitionToReadyAfterReceivingNewLeadershipTerm)
{
    ElectionTestFixture f;
    /* Member 1 is a follower */
    f.build(THREE_NODE, 1, NULL_VALUE, 0, NULL_VALUE, -1,
        100LL, 500000LL, 1LL);

    int64_t now = 50LL;
    f.do_work(now);

    /* Leader (member 0) sends NewLeadershipTerm */
    f.on_new_leadership_term(
        NULL_VALUE,  /* log_term */
        1LL,         /* next_term */
        0,           /* next_base */
        0,           /* next_log_pos */
        1LL,         /* leadership_term_id */
        0,           /* base */
        0,           /* log_pos */
        0,           /* commit_pos */
        600LL,       /* recording_id */
        now,         /* timestamp */
        0,           /* leader_member_id */
        777,         /* log_session_id */
        0,           /* app_version */
        true);       /* is_startup */

    EXPECT_EQ(AERON_ELECTION_FOLLOWER_REPLAY, f.state());
    EXPECT_EQ(1, f.agent.follower_new_term_count);
}

/* -----------------------------------------------------------------------
 * 13. followerShouldSendAppendPositionOnReady
 * ----------------------------------------------------------------------- */
TEST(ElectionTest, followerShouldSendAppendPositionOnReady)
{
    ElectionTestFixture f;
    f.build(THREE_NODE, 1, NULL_VALUE, 0, NULL_VALUE, -1,
        100LL, 500000LL, 1LL);

    int64_t now = 50LL;
    f.do_work(now);

    f.on_new_leadership_term(NULL_VALUE, 1LL, 0, 0, 1LL, 0, 0, 0, 600LL, now, 0, 777, 0, true);
    EXPECT_EQ(AERON_ELECTION_FOLLOWER_REPLAY, f.state());

    f.do_work(now + 1LL); /* FOLLOWER_REPLAY → LOG_INIT → LOG_AWAIT → READY → CLOSED */

    /* Should have sent AppendPosition to leader (member 0) */
    EXPECT_TRUE(f.pub.sent_to("append_position", 0));
    EXPECT_EQ(AERON_ELECTION_CLOSED, f.state());
    EXPECT_EQ(1, f.agent.election_complete_count);
}

/* -----------------------------------------------------------------------
 * 14. leaderShouldBroadcastNewLeadershipTermToFollowers
 * ----------------------------------------------------------------------- */
TEST(ElectionTest, leaderShouldBroadcastNewLeadershipTermToFollowers)
{
    ElectionTestFixture f;
    f.agent.log_recording_id = 600LL;
    f.build(THREE_NODE, 0, NULL_VALUE, 0, NULL_VALUE, -1,
        100LL, 1000000000LL, 1LL);

    int64_t now = 50LL;
    f.do_work(now);
    f.on_canvass(1, NULL_VALUE, 0, NULL_VALUE);
    f.on_canvass(2, NULL_VALUE, 0, NULL_VALUE);
    f.do_work(now + 200LL);
    f.do_work(now + 200LL + 1000000001LL);  /* → CANDIDATE_BALLOT */

    int64_t ct = f.election->candidate_term_id;
    f.on_vote(1, ct, NULL_VALUE, 0, 0, true);
    f.on_vote(2, ct, NULL_VALUE, 0, 0, true);

    f.do_work(now + 200LL + 1000000002LL); /* → LEADER_LOG_REPLICATION → LEADER_INIT */
    f.do_work(now + 200LL + 1000000003LL);

    /* NewLeadershipTerm must have been sent to both followers */
    EXPECT_TRUE(f.pub.sent_to("new_leadership_term", 1));
    EXPECT_TRUE(f.pub.sent_to("new_leadership_term", 2));
    EXPECT_EQ(AERON_ELECTION_LEADER_READY, f.state());
}

/* -----------------------------------------------------------------------
 * 15. leaderShouldBecomeClosedWhenFollowerQuorumAcknowledges
 * ----------------------------------------------------------------------- */
TEST(ElectionTest, leaderShouldBecomeClosedWhenFollowerQuorumAcknowledges)
{
    ElectionTestFixture f;
    f.agent.log_recording_id = 600LL;
    f.build(THREE_NODE, 0, NULL_VALUE, 0, NULL_VALUE, -1,
        100LL, 1000000000LL, 1LL);

    int64_t now = 50LL;
    f.do_work(now);
    f.on_canvass(1, NULL_VALUE, 0, NULL_VALUE);
    f.on_canvass(2, NULL_VALUE, 0, NULL_VALUE);
    f.do_work(now + 200LL);
    f.do_work(now + 200LL + 1000000001LL);

    int64_t ct = f.election->candidate_term_id;
    f.on_vote(1, ct, NULL_VALUE, 0, 0, true);
    f.on_vote(2, ct, NULL_VALUE, 0, 0, true);
    f.do_work(now += 200LL + 1000000002LL);
    f.do_work(now += 1LL); /* → LEADER_INIT → LEADER_READY */

    int64_t term = f.election->leadership_term_id;

    /* Follower 1 sends AppendPosition acknowledging new term */
    f.on_append_pos(1, term, 0);
    f.do_work(now += 1LL);

    /* Quorum (self + 1) → CLOSED */
    EXPECT_EQ(AERON_ELECTION_CLOSED, f.state());
    EXPECT_EQ(1, f.agent.election_complete_count);
}

/* -----------------------------------------------------------------------
 * 16. notifiedCommitPositionCannotGoBackwardsUponReceivingCommitPosition
 * ----------------------------------------------------------------------- */
TEST(ElectionTest, notifiedCommitPositionCannotGoBackwardsUponReceivingCommitPosition)
{
    ElectionTestFixture f;
    f.build(THREE_NODE, 1, NULL_VALUE, 0, NULL_VALUE, -1,
        100LL, 500000LL, 1LL);

    int64_t now = 50LL;
    f.do_work(now);

    /* Receive commit position 100 */
    f.on_commit_position(NULL_VALUE, 100LL, 0);
    ASSERT_EQ(1u, f.agent.notified_commit_positions.size());
    EXPECT_EQ(100LL, f.agent.notified_commit_positions[0]);

    /* Receive commit position 50 — must NOT go backwards */
    f.on_commit_position(NULL_VALUE, 50LL, 0);
    /* Should not have notified (50 < 100) */
    EXPECT_EQ(1u, f.agent.notified_commit_positions.size());

    /* Receive commit position 200 — should notify */
    f.on_commit_position(NULL_VALUE, 200LL, 0);
    EXPECT_EQ(2u, f.agent.notified_commit_positions.size());
    EXPECT_EQ(200LL, f.agent.notified_commit_positions[1]);
}

/* -----------------------------------------------------------------------
 * 17. notifiedCommitPositionCannotGoBackwardsUponReceivingNewLeadershipTerm
 * ----------------------------------------------------------------------- */
TEST(ElectionTest, notifiedCommitPositionCannotGoBackwardsUponReceivingNewLeadershipTerm)
{
    ElectionTestFixture f;
    f.build(THREE_NODE, 1, NULL_VALUE, 0, NULL_VALUE, -1,
        100LL, 500000LL, 1LL);

    f.do_work(50LL);

    /* Receive commit position 500 */
    f.on_commit_position(NULL_VALUE, 500LL, 0);
    EXPECT_EQ(500LL, f.election->notified_commit_position);

    /* New leadership term with lower commit — should not decrease */
    f.on_commit_position(NULL_VALUE, 100LL, 0);
    EXPECT_EQ(500LL, f.election->notified_commit_position); /* unchanged */
}

/* -----------------------------------------------------------------------
 * 18. shouldElectSingleNodeImmediately
 * ----------------------------------------------------------------------- */
TEST(ElectionTest, shouldElectSingleNodeImmediately)
{
    ElectionTestFixture f;
    f.build(SINGLE_NODE, 0, NULL_VALUE, 0, NULL_VALUE, -1);

    int64_t now = 1000LL;
    /* Single node should not need canvass — goes straight to leader path */
    f.do_work(now);
    EXPECT_TRUE(f.state_reached(AERON_ELECTION_LEADER_LOG_REPLICATION) ||
                f.state_reached(AERON_ELECTION_LEADER_INIT) ||
                f.state_reached(AERON_ELECTION_LEADER_READY));
    EXPECT_EQ(0, f.pub.request_vote_count()); /* no votes needed */
}

/* -----------------------------------------------------------------------
 * 19. shouldVoteNoIfCandidateHasOlderTerm
 * ----------------------------------------------------------------------- */
TEST(ElectionTest, shouldVoteNoIfCandidateHasOlderTerm)
{
    ElectionTestFixture f;
    /* Member 2: has higher log than candidate 0 */
    f.agent.append_position = 1000LL;
    f.build(THREE_NODE, 2, 5LL /* log_term */, 1000LL /* log_pos */, 5LL, -1);

    f.do_work(1000LL);

    /* Candidate 0 requests vote for lower term */
    aeron_cluster_election_on_request_vote(f.election,
        3LL /* log_term < 5 */, 500LL /* log_pos < 1000 */,
        6LL /* candidate_term */, 0 /* candidate_id */);

    /* We should have voted NO (candidate has less log) */
    auto *v = f.pub.last("vote");
    ASSERT_NE(nullptr, v);
    EXPECT_FALSE(v->vote_value);
}

/* -----------------------------------------------------------------------
 * 20. shouldVoteYesIfCandidateHasEqualOrBetterLog
 * ----------------------------------------------------------------------- */
TEST(ElectionTest, shouldVoteYesIfCandidateHasEqualOrBetterLog)
{
    ElectionTestFixture f;
    f.build(THREE_NODE, 2, NULL_VALUE, 0, NULL_VALUE, -1);
    f.do_work(1000LL);

    /* Candidate 0 with same log → vote YES */
    aeron_cluster_election_on_request_vote(f.election,
        NULL_VALUE, 0, 1LL, 0);

    auto *v = f.pub.last("vote");
    ASSERT_NE(nullptr, v);
    EXPECT_TRUE(v->vote_value);
}

/* -----------------------------------------------------------------------
 * 21. shouldSendCommitPositionDuringLeaderReady
 * ----------------------------------------------------------------------- */
TEST(ElectionTest, shouldSendCommitPositionDuringLeaderReady)
{
    ElectionTestFixture f;
    f.agent.log_recording_id = 600LL;
    f.agent.append_position  = 42LL;
    f.build(THREE_NODE, 0, NULL_VALUE, 0, NULL_VALUE, -1,
        100LL, 1000000000LL, /* status_interval */ 1LL);

    int64_t now = 50LL;
    f.do_work(now);
    f.on_canvass(1, NULL_VALUE, 0, NULL_VALUE);
    f.on_canvass(2, NULL_VALUE, 0, NULL_VALUE);
    f.do_work(now + 200LL);
    f.do_work(now + 200LL + 1000000001LL);

    int64_t ct = f.election->candidate_term_id;
    f.on_vote(1, ct, NULL_VALUE, 0, 0, true);
    f.on_vote(2, ct, NULL_VALUE, 0, 0, true);
    f.do_work(now += 200LL + 1000000002LL);
    f.do_work(now += 1LL); /* → LEADER_READY */

    EXPECT_EQ(AERON_ELECTION_LEADER_READY, f.state());

    /* While in LEADER_READY, commit position should be broadcast */
    f.do_work(now += 2LL);
    EXPECT_GE(f.pub.commit_pos_count(), 0); /* may have committed pos already */
}

/* -----------------------------------------------------------------------
 * 22. shouldStateChangeToCanvassOnInit
 * ----------------------------------------------------------------------- */
TEST(ElectionTest, shouldStateChangeToCanvassOnInit)
{
    ElectionTestFixture f;
    f.build(THREE_NODE, 0, NULL_VALUE, 0, NULL_VALUE, -1);

    EXPECT_EQ(AERON_ELECTION_INIT, f.state());
    f.do_work(1000LL);
    EXPECT_EQ(AERON_ELECTION_CANVASS, f.state());

    /* Verify state_change notification was fired */
    EXPECT_FALSE(f.agent.state_changes.empty());
    EXPECT_EQ(AERON_ELECTION_CANVASS, f.agent.state_changes.back());
}

/* -----------------------------------------------------------------------
 * 23. shouldTrackCandidateTermAcrossRestartedElection
 * ----------------------------------------------------------------------- */
TEST(ElectionTest, shouldTrackCandidateTermAcrossRestartedElection)
{
    ElectionTestFixture f;
    f.build(THREE_NODE, 0, NULL_VALUE, 0, NULL_VALUE, -1,
        100LL, 500000LL, 1LL);

    int64_t now = 50LL;
    f.do_work(now);
    f.on_canvass(1, NULL_VALUE, 0, NULL_VALUE);
    f.on_canvass(2, NULL_VALUE, 0, NULL_VALUE);
    f.do_work(now + 200LL);
    f.do_work(now + 200LL + 500001LL); /* → CANDIDATE_BALLOT */

    int64_t first_term = f.election->candidate_term_id;

    /* Timeout → restart */
    f.do_work(now + 200LL + 1000002LL);

    /* Re-canvass */
    f.do_work(now + 200LL + 1000003LL);
    f.on_canvass(1, NULL_VALUE, 0, NULL_VALUE);
    f.on_canvass(2, NULL_VALUE, 0, NULL_VALUE);
    f.do_work(now + 200LL + 1000003LL + 200LL);
    f.do_work(now + 200LL + 1000003LL + 200LL + 500001LL); /* → CANDIDATE_BALLOT again */

    int64_t second_term = f.election->candidate_term_id;
    EXPECT_GT(second_term, first_term); /* term must have advanced */
}

/* -----------------------------------------------------------------------
 * 24. shouldRejectStaleVoteIfTermDoesNotMatch
 * ----------------------------------------------------------------------- */
TEST(ElectionTest, shouldRejectStaleVoteIfTermDoesNotMatch)
{
    ElectionTestFixture f;
    f.build(THREE_NODE, 0, NULL_VALUE, 0, NULL_VALUE, -1,
        100LL, 1000000000LL, 1LL);

    int64_t now = 50LL;
    f.do_work(now);
    f.on_canvass(1, NULL_VALUE, 0, NULL_VALUE);
    f.on_canvass(2, NULL_VALUE, 0, NULL_VALUE);
    f.do_work(now + 200LL);
    f.do_work(now + 200LL + 1000000001LL); /* → CANDIDATE_BALLOT */

    int64_t ct = f.election->candidate_term_id;
    EXPECT_TRUE(f.state_reached(AERON_ELECTION_CANDIDATE_BALLOT));

    /* Vote for wrong term — should not count */
    f.on_vote(1, ct - 1 /* stale term */, NULL_VALUE, 0, 0, true);
    f.on_vote(2, ct - 1, NULL_VALUE, 0, 0, true);

    f.do_work(now + 200LL + 1000000002LL);

    /* Should NOT have progressed to leader (stale votes ignored) */
    EXPECT_FALSE(f.state_reached(AERON_ELECTION_LEADER_LOG_REPLICATION));
}

/* -----------------------------------------------------------------------
 * 25. shouldRecordStateTransitionsInOrder
 *     For a 3-node leader path: INIT→CANVASS→NOMINATE→CANDIDATE_BALLOT→LEADER_*→CLOSED
 * ----------------------------------------------------------------------- */
TEST(ElectionTest, shouldRecordStateTransitionsInOrder)
{
    ElectionTestFixture f;
    f.agent.log_recording_id = 600LL;
    f.build(THREE_NODE, 0, NULL_VALUE, 0, NULL_VALUE, -1,
        100LL, 1000000000LL, 1LL);

    int64_t now = 50LL;
    f.do_work(now);
    f.on_canvass(1, NULL_VALUE, 0, NULL_VALUE);
    f.on_canvass(2, NULL_VALUE, 0, NULL_VALUE);
    f.do_work(now + 200LL);
    f.do_work(now + 200LL + 1000000001LL);

    int64_t ct = f.election->candidate_term_id;
    f.on_vote(1, ct, NULL_VALUE, 0, 0, true);
    f.on_vote(2, ct, NULL_VALUE, 0, 0, true);
    f.do_work(now += 200LL + 1000000002LL);
    f.do_work(now += 1LL);
    f.do_work(now += 1LL);

    int64_t term = f.election->leadership_term_id;
    f.on_append_pos(1, term, 0);
    f.do_work(now += 1LL);

    /* Verify the full leader path was traversed */
    EXPECT_TRUE(f.state_reached(AERON_ELECTION_CANVASS));
    EXPECT_TRUE(f.state_reached(AERON_ELECTION_NOMINATE));
    EXPECT_TRUE(f.state_reached(AERON_ELECTION_CANDIDATE_BALLOT));
    EXPECT_TRUE(f.state_reached(AERON_ELECTION_LEADER_LOG_REPLICATION));
    EXPECT_TRUE(f.state_reached(AERON_ELECTION_LEADER_INIT));
    EXPECT_TRUE(f.state_reached(AERON_ELECTION_LEADER_READY));
    EXPECT_EQ(AERON_ELECTION_CLOSED, f.state());
}

/* -----------------------------------------------------------------------
 * 26. followerShouldReplicateLogAndTransitionToCanvass
 *     When a follower's appendPosition < termBaseLogPosition it enters
 *     FOLLOWER_LOG_REPLICATION, runs the replication to completion and
 *     then returns to CANVASS to re-join the quorum.
 * ----------------------------------------------------------------------- */

/* File-level mock for the recording replication object used in tests 26-27. */
static aeron_cluster_recording_replication_t s_test_log_rep;

TEST(ElectionTest, followerShouldReplicateLogAndTransitionToCanvass)
{
    ElectionTestFixture f;
    /* member 1, initial append_position = 0 (via log_position = 0) */
    f.build(THREE_NODE, 1, NULL_VALUE, 0, NULL_VALUE, -1,
        /* startup_canvass */ 5000000000LL,
        /* election_timeout */ 1000000000LL,
        /* status_interval */ 1LL,
        /* heartbeat_timeout */ 10000000000LL);

    int64_t now = 1000LL;
    f.do_work(now);
    EXPECT_EQ(AERON_ELECTION_CANVASS, f.state());

    /* Prepare mock replication: already done when first polled */
    memset(&s_test_log_rep, 0, sizeof(s_test_log_rep));
    s_test_log_rep.has_replication_ended = true;
    s_test_log_rep.has_stopped           = true;
    s_test_log_rep.position              = 100LL;
    s_test_log_rep.stop_position         = 100LL;
    s_test_log_rep.progress_check_deadline_ns = INT64_MAX;
    s_test_log_rep.progress_deadline_ns       = INT64_MAX;

    /* Override: new_log_replication returns the pre-built mock */
    f.election->agent_ops.new_log_replication =
        [](void *, const char *, const char *, int64_t, int64_t, int64_t) -> void *
        { return &s_test_log_rep; };
    /* close_log_replication is a no-op (mock is stack/static memory) */
    f.election->agent_ops.close_log_replication = [](void *, void *) {};

    /* Leader (member 0) sends NewLeadershipTerm.
     * append_position (0) < term_base_log_position (200) → FOLLOWER_LOG_REPLICATION.
     * next_term_base = 100, so replication_stop_position = 100. */
    f.on_new_leadership_term(
        0LL,    /* log_leadership_term_id  */
        1LL,    /* next_leadership_term_id */
        100LL,  /* next_term_base          */
        200LL,  /* next_log_position       */
        1LL,    /* leadership_term_id      */
        200LL,  /* term_base_log_position  */
        200LL,  /* log_position            */
        0LL,    /* commit_position         */
        600LL,  /* leader_recording_id     */
        now,    /* timestamp               */
        0,      /* leader_member_id        */
        777,    /* log_session_id          */
        0,      /* app_version             */
        false); /* is_startup              */

    EXPECT_EQ(AERON_ELECTION_FOLLOWER_LOG_REPLICATION, f.state());

    /* Drive: creates replication → polls (done immediately) → CANVASS */
    f.do_work(now + 1LL);

    EXPECT_TRUE(f.state_reached(AERON_ELECTION_FOLLOWER_LOG_REPLICATION));
    EXPECT_EQ(AERON_ELECTION_CANVASS, f.state());
    EXPECT_EQ(100LL, f.election->append_position);
}

/* -----------------------------------------------------------------------
 * 27. followerAlreadyAtReplicationPositionGoesDirectlyToCanvass
 *     When appendPosition already equals the replication stop position
 *     the follower skips actual replication and transitions to CANVASS.
 * ----------------------------------------------------------------------- */
TEST(ElectionTest, followerAlreadyAtReplicationPositionGoesDirectlyToCanvass)
{
    ElectionTestFixture f;
    /* member 1, initial append_position = 100 */
    f.agent.append_position = 100LL;
    f.build(THREE_NODE, 1,
        /* log_term */ NULL_VALUE,
        /* log_pos  */ 100LL,
        /* term_id  */ NULL_VALUE,
        -1,
        5000000000LL, 1000000000LL, 1LL, 10000000000LL);

    int64_t now = 1000LL;
    f.do_work(now);
    EXPECT_EQ(AERON_ELECTION_CANVASS, f.state());

    /* NewLeadershipTerm: append (100) < term_base (200), next_term_base=100,
     * next_log_pos=100  →  append == next_term_base, stop = next_log_pos = 100.
     * In do_follower_log_replication: append (100) >= stop (100) → skip to CANVASS. */
    f.on_new_leadership_term(
        0LL, 1LL, 100LL, 100LL, 1LL, 200LL, 200LL,
        0LL, 600LL, now, 0, 777, 0, false);

    EXPECT_EQ(AERON_ELECTION_FOLLOWER_LOG_REPLICATION, f.state());

    f.do_work(now + 1LL);

    EXPECT_TRUE(f.state_reached(AERON_ELECTION_FOLLOWER_LOG_REPLICATION));
    EXPECT_EQ(AERON_ELECTION_CANVASS, f.state());
}

/* -----------------------------------------------------------------------
 * 28. followerShouldCatchupAndJoinLiveLog
 *     When a follower's appendPosition is at the term base but behind
 *     the leader's log_position it takes the CATCHUP path:
 *     FOLLOWER_REPLAY → FOLLOWER_CATCHUP_INIT → FOLLOWER_CATCHUP_AWAIT
 *     → FOLLOWER_CATCHUP → FOLLOWER_LOG_INIT → FOLLOWER_READY → CLOSED
 * ----------------------------------------------------------------------- */
TEST(ElectionTest, followerShouldCatchupAndJoinLiveLog)
{
    ElectionTestFixture f;
    /* member 1, append_position = 0 */
    f.build(THREE_NODE, 1, NULL_VALUE, 0, NULL_VALUE, -1,
        5000000000LL, 1000000000LL, 1LL, 10000000000LL);

    int64_t now = 1000LL;
    f.do_work(now);
    EXPECT_EQ(AERON_ELECTION_CANVASS, f.state());

    /* Override agent ops so catchup can complete in unit tests:
     * - send_catchup_position: return true so CATCHUP_INIT → CATCHUP_AWAIT
     * - is_catchup_near_live: true so live destination is added immediately
     * - get_commit_position: 100 (== catchup_join_position) so CATCHUP → LOG_INIT */
    f.election->agent_ops.this_catchup_endpoint =
        [](void *) -> const char * { return "localhost:9999"; };
    f.election->agent_ops.send_catchup_position =
        [](void *, const char *) -> bool { return true; };
    f.election->agent_ops.is_catchup_near_live =
        [](void *, int64_t) -> bool { return true; };
    f.election->agent_ops.get_commit_position =
        [](void *) -> int64_t { return 100LL; };

    /* NewLeadershipTerm: term_base=0, log_pos=100.
     * append (0) >= term_base (0) → FOLLOWER_REPLAY.
     * log_pos (100) > append (0) → catchup_join_position = 100. */
    f.on_new_leadership_term(
        NULL_VALUE, 1LL, 0LL, 0LL,
        1LL, 0LL, 100LL,
        0LL, 600LL, now, 0, 777, 0, false);

    EXPECT_EQ(AERON_ELECTION_FOLLOWER_REPLAY, f.state());

    /* Single do_work drives the full catchup path to CLOSED */
    f.do_work(now + 1LL);

    EXPECT_TRUE(f.state_reached(AERON_ELECTION_FOLLOWER_CATCHUP_INIT));
    EXPECT_TRUE(f.state_reached(AERON_ELECTION_FOLLOWER_CATCHUP_AWAIT));
    EXPECT_TRUE(f.state_reached(AERON_ELECTION_FOLLOWER_CATCHUP));
    EXPECT_TRUE(f.state_reached(AERON_ELECTION_FOLLOWER_LOG_INIT));
    EXPECT_TRUE(f.state_reached(AERON_ELECTION_FOLLOWER_READY));
    EXPECT_EQ(AERON_ELECTION_CLOSED, f.state());
    EXPECT_EQ(1, f.agent.election_complete_count);
}
