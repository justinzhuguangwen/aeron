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
#include <cstring>
#include <cstdlib>
#include <cstdint>

extern "C"
{
#include "aeron_cluster_member.h"
#include "aeron_common.h"
}

/* -----------------------------------------------------------------------
 * Helper
 * ----------------------------------------------------------------------- */
static void member_set(aeron_cluster_member_t *m, int32_t id,
                        int64_t term_id, int64_t log_pos, int64_t ts = 0)
{
    m->id = id;
    m->leadership_term_id = term_id;
    m->log_position  = log_pos;
    m->time_of_last_append_position_ns = ts;
    m->candidate_term_id = -1;
    m->vote = -1;
}

/* -----------------------------------------------------------------------
 * Fixture
 * ----------------------------------------------------------------------- */
class ClusterMemberTest : public ::testing::Test
{
protected:
    void TearDown() override
    {
        if (m_members) { aeron_cluster_members_free(m_members, m_count); }
    }
    void parse(const char *str)
    {
        if (m_members) { aeron_cluster_members_free(m_members, m_count); m_members = nullptr; }
        ASSERT_EQ(0, aeron_cluster_members_parse(str, &m_members, &m_count));
    }

    aeron_cluster_member_t *m_members = nullptr;
    int m_count = 0;
};

/* -----------------------------------------------------------------------
 * ClusterMemberTest cases
 * ----------------------------------------------------------------------- */

TEST_F(ClusterMemberTest, shouldParseCorrectly)
{
    parse("0,in:cons:log:catch:arch");
    ASSERT_EQ(1, m_count);
    EXPECT_EQ(0, m_members[0].id);
    EXPECT_STREQ("in",   m_members[0].ingress_endpoint);
    EXPECT_STREQ("cons", m_members[0].consensus_endpoint);
    EXPECT_STREQ("log",  m_members[0].log_endpoint);
    EXPECT_STREQ("catch",m_members[0].catchup_endpoint);
    EXPECT_STREQ("arch", m_members[0].archive_endpoint);
}

TEST_F(ClusterMemberTest, shouldParseThreeMembers)
{
    parse("0,h0:9010:h0:9020:h0:9030:h0:9040:h0:8010|"
          "1,h1:9010:h1:9020:h1:9030:h1:9040:h1:8010|"
          "2,h2:9010:h2:9020:h2:9030:h2:9040:h2:8010");
    ASSERT_EQ(3, m_count);
    for (int i = 0; i < 3; i++) { EXPECT_EQ(i, m_members[i].id); }
}

TEST_F(ClusterMemberTest, shouldFindMemberById)
{
    parse("0,h0:e0:l0:c0:a0|1,h1:e1:l1:c1:a1|2,h2:e2:l2:c2:a2");
    ASSERT_EQ(3, m_count);
    auto *m = aeron_cluster_member_find_by_id(m_members, m_count, 1);
    ASSERT_NE(nullptr, m);
    EXPECT_EQ(1, m->id);
    EXPECT_EQ(nullptr, aeron_cluster_member_find_by_id(m_members, m_count, 99));
}

TEST_F(ClusterMemberTest, shouldComputeQuorumThreshold)
{
    EXPECT_EQ(1, aeron_cluster_member_quorum_threshold(1));
    EXPECT_EQ(2, aeron_cluster_member_quorum_threshold(2));
    EXPECT_EQ(2, aeron_cluster_member_quorum_threshold(3));
    EXPECT_EQ(3, aeron_cluster_member_quorum_threshold(4));
    EXPECT_EQ(3, aeron_cluster_member_quorum_threshold(5));
    EXPECT_EQ(4, aeron_cluster_member_quorum_threshold(6));
    EXPECT_EQ(4, aeron_cluster_member_quorum_threshold(7));
}

TEST_F(ClusterMemberTest, shouldComputeQuorumPositionWithAllActive)
{
    parse("0,a:b:c:d:e|1,a:b:c:d:e|2,a:b:c:d:e");
    const int64_t now_ns = 1000000LL;
    const int64_t timeout = 10000000000LL;  /* 10s */

    m_members[0].log_position = 100; m_members[0].time_of_last_append_position_ns = now_ns;
    m_members[1].log_position = 200; m_members[1].time_of_last_append_position_ns = now_ns;
    m_members[2].log_position = 300; m_members[2].time_of_last_append_position_ns = now_ns;

    /* Quorum (2 of 3) = second highest = 200 */
    int64_t pos = aeron_cluster_member_quorum_position(m_members, m_count, now_ns, timeout);
    EXPECT_EQ(200, pos);
}

TEST_F(ClusterMemberTest, shouldOnlyConsiderActiveNodesForQuorumPosition)
{
    parse("0,a:b:c:d:e|1,a:b:c:d:e|2,a:b:c:d:e");
    const int64_t now_ns     = 100000000LL;
    const int64_t timeout    = 10000000LL;  /* 10ms */

    /* Member 2 timed out (last update was 0, now is 100ms > 10ms timeout) */
    m_members[0].log_position = 100; m_members[0].time_of_last_append_position_ns = now_ns;
    m_members[1].log_position = 200; m_members[1].time_of_last_append_position_ns = now_ns;
    m_members[2].log_position = 300; m_members[2].time_of_last_append_position_ns = 0; /* timed out */

    /* Quorum with member 2 as -1: sorted = [200, 100, -1], quorum index 1 = 100 */
    int64_t pos = aeron_cluster_member_quorum_position(m_members, m_count, now_ns, timeout);
    EXPECT_EQ(100, pos);
}

TEST_F(ClusterMemberTest, shouldCountVotesForCandidateTerm)
{
    parse("0,a:b:c:d:e|1,a:b:c:d:e|2,a:b:c:d:e");
    m_members[0].candidate_term_id = 5;
    m_members[1].candidate_term_id = 5;
    m_members[2].candidate_term_id = 4;  /* voted for different term */
    EXPECT_EQ(2, aeron_cluster_member_count_votes(m_members, m_count, 5));
    EXPECT_EQ(1, aeron_cluster_member_count_votes(m_members, m_count, 4));
    EXPECT_EQ(0, aeron_cluster_member_count_votes(m_members, m_count, 99));
}

TEST_F(ClusterMemberTest, singleMemberAlwaysQuorum)
{
    parse("0,a:b:c:d:e");
    const int64_t now_ns  = 1000LL;
    const int64_t timeout = 9999999999LL;
    m_members[0].log_position = 42;
    m_members[0].time_of_last_append_position_ns = now_ns;
    /* Single node: quorum position = its own position */
    EXPECT_EQ(42, aeron_cluster_member_quorum_position(m_members, m_count, now_ns, timeout));
}

TEST_F(ClusterMemberTest, noMembersActiveReturnsMinusOne)
{
    parse("0,a:b:c:d:e|1,a:b:c:d:e|2,a:b:c:d:e");
    const int64_t now_ns  = 100000000LL;
    const int64_t timeout = 1LL;  /* all timed out */
    m_members[0].time_of_last_append_position_ns = 0;
    m_members[1].time_of_last_append_position_ns = 0;
    m_members[2].time_of_last_append_position_ns = 0;
    int64_t pos = aeron_cluster_member_quorum_position(m_members, m_count, now_ns, timeout);
    EXPECT_EQ(-1, pos);
}

TEST_F(ClusterMemberTest, shouldDetermineQuorumPositionWithFiveNodes)
{
    parse("0,a:b:c:d:e|1,a:b:c:d:e|2,a:b:c:d:e|3,a:b:c:d:e|4,a:b:c:d:e");
    const int64_t now_ns  = 1000000LL;
    const int64_t timeout = 10000000000LL;

    m_members[0].log_position = 100; m_members[0].time_of_last_append_position_ns = now_ns;
    m_members[1].log_position = 200; m_members[1].time_of_last_append_position_ns = now_ns;
    m_members[2].log_position = 300; m_members[2].time_of_last_append_position_ns = now_ns;
    m_members[3].log_position = 400; m_members[3].time_of_last_append_position_ns = now_ns;
    m_members[4].log_position = 500; m_members[4].time_of_last_append_position_ns = now_ns;

    /* Quorum = 3; third highest = 300 */
    EXPECT_EQ(300, aeron_cluster_member_quorum_position(m_members, m_count, now_ns, timeout));
}

TEST_F(ClusterMemberTest, isQuorumCandidateFalseWhenNoPosition)
{
    parse("0,a:b:c:d:e");
    m_members[0].log_position = -1; /* no position */
    EXPECT_FALSE(aeron_cluster_member_is_quorum_candidate(&m_members[0], 1000LL, 9999999999LL));
}

TEST_F(ClusterMemberTest, isQuorumCandidateFalseWhenTimedOut)
{
    parse("0,a:b:c:d:e");
    m_members[0].log_position = 100;
    m_members[0].time_of_last_append_position_ns = 0;
    /* timeout = 1ns, now = 1000000ns → timed out */
    EXPECT_FALSE(aeron_cluster_member_is_quorum_candidate(&m_members[0], 1000000LL, 1LL));
}

TEST_F(ClusterMemberTest, isQuorumCandidateTrueWhenRecentAndHasPosition)
{
    parse("0,a:b:c:d:e");
    m_members[0].log_position = 42;
    m_members[0].time_of_last_append_position_ns = 900000LL;
    EXPECT_TRUE(aeron_cluster_member_is_quorum_candidate(&m_members[0], 1000000LL, 9999999999LL));
}

TEST_F(ClusterMemberTest, shouldCountVotesForMultipleTerms)
{
    parse("0,a:b:c:d:e|1,a:b:c:d:e|2,a:b:c:d:e|3,a:b:c:d:e|4,a:b:c:d:e");
    for (int i = 0; i < m_count; i++) { m_members[i].candidate_term_id = static_cast<int64_t>(i < 3 ? 10 : 11); }
    EXPECT_EQ(3, aeron_cluster_member_count_votes(m_members, m_count, 10));
    EXPECT_EQ(2, aeron_cluster_member_count_votes(m_members, m_count, 11));
    EXPECT_EQ(0, aeron_cluster_member_count_votes(m_members, m_count, 99));
}

TEST_F(ClusterMemberTest, quorumThresholdForLargeCluster)
{
    EXPECT_EQ(5, aeron_cluster_member_quorum_threshold(9));
    EXPECT_EQ(6, aeron_cluster_member_quorum_threshold(11));
}

TEST_F(ClusterMemberTest, quorumPositionWithTwoNodesTimedOut)
{
    parse("0,a:b:c:d:e|1,a:b:c:d:e|2,a:b:c:d:e");
    const int64_t now_ns  = 100000000LL;
    const int64_t timeout = 1LL;  /* everyone timed out */

    m_members[0].log_position = 100;
    m_members[1].log_position = 200;
    m_members[2].log_position = 300;
    /* All timed out → quorum position = -1 */
    int64_t pos = aeron_cluster_member_quorum_position(m_members, m_count, now_ns, timeout);
    EXPECT_EQ(-1, pos);
}

TEST_F(ClusterMemberTest, parseEmptyTopologyReturnsZero)
{
    if (m_members) { aeron_cluster_members_free(m_members, m_count); m_members = nullptr; }
    ASSERT_EQ(0, aeron_cluster_members_parse(nullptr, &m_members, &m_count));
    EXPECT_EQ(0, m_count);
    EXPECT_EQ(nullptr, m_members);
}

TEST_F(ClusterMemberTest, shouldRankClusterStart)
{
    parse("0,a:b:c:d:e|1,a:b:c:d:e|2,a:b:c:d:e");
    const int64_t now_ns  = 0LL;
    const int64_t timeout = 10LL;
    m_members[0].log_position = 0;
    m_members[1].log_position = 0;
    m_members[2].log_position = 0;
    EXPECT_EQ(0, aeron_cluster_member_quorum_position(m_members, m_count, now_ns, timeout));
}

TEST_F(ClusterMemberTest, isUnanimousCandidateFalseIfMemberHasNoLogPosition)
{
    parse("1,a:b:c:d:e|2,a:b:c:d:e|3,a:b:c:d:e");
    aeron_cluster_member_t candidate{};
    member_set(&candidate, 4, 2, 1000);

    member_set(&m_members[0], 1, 2, 100);
    member_set(&m_members[1], 2, 8, -1);  /* no position */
    member_set(&m_members[2], 3, 1, 1);

    EXPECT_FALSE(aeron_cluster_member_is_unanimous_candidate(
        m_members, m_count, &candidate, -1));
}

TEST_F(ClusterMemberTest, isUnanimousCandidateFalseIfMemberHasMoreLog)
{
    parse("1,a:b:c:d:e|2,a:b:c:d:e|3,a:b:c:d:e");
    aeron_cluster_member_t candidate{};
    member_set(&candidate, 4, 10, 800);

    member_set(&m_members[0], 1, 2, 100);
    member_set(&m_members[1], 2, 8, 6);
    member_set(&m_members[2], 3, 11, 1000);  /* better than candidate */

    EXPECT_FALSE(aeron_cluster_member_is_unanimous_candidate(
        m_members, m_count, &candidate, -1));
}

TEST_F(ClusterMemberTest, isUnanimousCandidateFalseIfGracefulLeaderSkipped)
{
    parse("1,a:b:c:d:e|2,a:b:c:d:e");
    aeron_cluster_member_t candidate{};
    member_set(&candidate, 2, 2, 100);

    member_set(&m_members[0], 1, 2, 100);
    member_set(&m_members[1], 2, 2, 100);

    /* gracefulClosedLeaderId=1 → only member 2 counts; 1 < quorum(2)=2 → false */
    EXPECT_FALSE(aeron_cluster_member_is_unanimous_candidate(
        m_members, m_count, &candidate, 1));
}

TEST_F(ClusterMemberTest, isUnanimousCandidateTrueIfCandidateHasBestLog)
{
    parse("10,a:b:c:d:e|20,a:b:c:d:e|30,a:b:c:d:e");
    aeron_cluster_member_t candidate{};
    member_set(&candidate, 2, 10, 800);

    member_set(&m_members[0], 10, 2, 100);
    member_set(&m_members[1], 20, 8, 6);
    member_set(&m_members[2], 30, 10, 800);

    EXPECT_TRUE(aeron_cluster_member_is_unanimous_candidate(
        m_members, m_count, &candidate, -1));
}

TEST_F(ClusterMemberTest, isQuorumCandidateFalseWhenQuorumNotReached)
{
    parse("10,a:b:c:d:e|20,a:b:c:d:e|30,a:b:c:d:e|40,a:b:c:d:e|50,a:b:c:d:e");
    aeron_cluster_member_t candidate{};
    member_set(&candidate, 2, 10, 800);

    member_set(&m_members[0], 10, 2, 100);
    member_set(&m_members[1], 20, 18, 600);
    member_set(&m_members[2], 30, 10, 800);
    member_set(&m_members[3], 40, 19, 800);
    member_set(&m_members[4], 50, 10, 1000);  /* better than candidate */

    EXPECT_FALSE(aeron_cluster_member_is_quorum_candidate_for(
        m_members, m_count, &candidate));
}

TEST_F(ClusterMemberTest, isQuorumCandidateTrueWhenQuorumReached)
{
    parse("10,a:b:c:d:e|20,a:b:c:d:e|30,a:b:c:d:e|40,a:b:c:d:e|50,a:b:c:d:e");
    aeron_cluster_member_t candidate{};
    member_set(&candidate, 2, 10, 800);

    member_set(&m_members[0], 10, 2, 100);
    member_set(&m_members[1], 20, 18, 600);
    member_set(&m_members[2], 30, 10, 800);
    member_set(&m_members[3], 40, 9, 800);
    member_set(&m_members[4], 50, 10, 700);

    EXPECT_TRUE(aeron_cluster_member_is_quorum_candidate_for(
        m_members, m_count, &candidate));
}

TEST_F(ClusterMemberTest, isQuorumLeaderReturnsTrueWhenQuorumReached)
{
    const int64_t ct = -5;
    parse("1,a:b:c:d:e|2,a:b:c:d:e|3,a:b:c:d:e|4,a:b:c:d:e|5,a:b:c:d:e");
    m_members[0].candidate_term_id = ct;   m_members[0].vote = 1;   /* YES */
    m_members[1].candidate_term_id = ct*2; m_members[1].vote = 0;   /* NO, different term */
    m_members[2].candidate_term_id = ct;   m_members[2].vote = -1;  /* null */
    m_members[3].candidate_term_id = ct;   m_members[3].vote = 1;   /* YES */
    m_members[4].candidate_term_id = ct;   m_members[4].vote = 1;   /* YES */

    EXPECT_TRUE(aeron_cluster_member_is_quorum_leader(m_members, m_count, ct));
}

TEST_F(ClusterMemberTest, isQuorumLeaderReturnsFalseOnNegativeVote)
{
    const int64_t ct = 8;
    parse("1,a:b:c:d:e|2,a:b:c:d:e|3,a:b:c:d:e|4,a:b:c:d:e|5,a:b:c:d:e");
    m_members[0].candidate_term_id = ct; m_members[0].vote = 1;
    m_members[1].candidate_term_id = ct; m_members[1].vote = 0;  /* explicit NO */
    m_members[2].candidate_term_id = ct; m_members[2].vote = 1;
    m_members[3].candidate_term_id = ct; m_members[3].vote = 1;
    m_members[4].candidate_term_id = ct; m_members[4].vote = 1;

    EXPECT_FALSE(aeron_cluster_member_is_quorum_leader(m_members, m_count, ct));
}

TEST_F(ClusterMemberTest, hasQuorumAtPositionTrue)
{
    const int64_t now_ns = 1000LL;
    const int64_t timeout = 9999999LL;
    const int64_t lt = 5LL;
    const int64_t pos = 100LL;

    parse("0,a:b:c:d:e|1,a:b:c:d:e|2,a:b:c:d:e");
    m_members[0].leadership_term_id = lt; m_members[0].log_position = 100;
    m_members[0].time_of_last_append_position_ns = now_ns;
    m_members[1].leadership_term_id = lt; m_members[1].log_position = 150;
    m_members[1].time_of_last_append_position_ns = now_ns;
    m_members[2].leadership_term_id = lt; m_members[2].log_position = 50;
    m_members[2].time_of_last_append_position_ns = now_ns;

    EXPECT_TRUE(aeron_cluster_member_has_quorum_at_position(
        m_members, m_count, lt, pos, now_ns, timeout));
}

TEST_F(ClusterMemberTest, hasQuorumAtPositionFalse)
{
    const int64_t now_ns = 1000LL;
    const int64_t timeout = 9999999LL;
    parse("0,a:b:c:d:e|1,a:b:c:d:e|2,a:b:c:d:e");
    m_members[0].leadership_term_id = 5; m_members[0].log_position = 50;
    m_members[0].time_of_last_append_position_ns = now_ns;
    m_members[1].leadership_term_id = 5; m_members[1].log_position = 50;
    m_members[1].time_of_last_append_position_ns = now_ns;
    m_members[2].leadership_term_id = 5; m_members[2].log_position = 50;
    m_members[2].time_of_last_append_position_ns = now_ns;

    /* All positions (50) are below the required position (100) — no quorum */
    EXPECT_FALSE(aeron_cluster_member_has_quorum_at_position(
        m_members, m_count, 5LL, 100LL, now_ns, timeout));
}

TEST_F(ClusterMemberTest, encodeAsString)
{
    parse("0,i0:c0:l0:cu0:a0|1,i1:c1:l1:cu1:a1");
    char buf[512];
    int n = aeron_cluster_members_encode_as_string(m_members, m_count, buf, sizeof(buf));
    EXPECT_GT(n, 0);
    EXPECT_STREQ(buf, "0,i0,c0,l0,cu0,a0|1,i1,c1,l1,cu1,a1");
}

TEST_F(ClusterMemberTest, encodeAsStringJavaFormat)
{
    parse("0,i0,c0,l0,cu0,a0|1,i1,c1,l1,cu1,a1");
    char buf[512];
    int n = aeron_cluster_members_encode_as_string(m_members, m_count, buf, sizeof(buf));
    EXPECT_GT(n, 0);
    EXPECT_STREQ(buf, "0,i0,c0,l0,cu0,a0|1,i1,c1,l1,cu1,a1");
}

TEST_F(ClusterMemberTest, parseOptionalResponseEndpoints)
{
    parse("0,ingress,consensus,log,catchup,archive,archResponse,egressResponse");
    ASSERT_EQ(1, m_count);
    EXPECT_STREQ(m_members[0].ingress_endpoint, "ingress");
    EXPECT_STREQ(m_members[0].archive_endpoint, "archive");
    EXPECT_STREQ(m_members[0].archive_response_endpoint, "archResponse");
    EXPECT_STREQ(m_members[0].egress_response_endpoint, "egressResponse");
}

TEST_F(ClusterMemberTest, ingressEndpoints)
{
    parse("0,h0:c0:l0:cu0:a0|1,h1:c1:l1:cu1:a1");
    char buf[256];
    int n = aeron_cluster_members_ingress_endpoints(m_members, m_count, buf, sizeof(buf));
    EXPECT_GT(n, 0);
    EXPECT_STREQ(buf, "0=h0,1=h1");
}

TEST_F(ClusterMemberTest, setIsLeader)
{
    parse("0,a:b:c:d:e|1,a:b:c:d:e|2,a:b:c:d:e");
    aeron_cluster_members_set_is_leader(m_members, m_count, 1);
    EXPECT_FALSE(m_members[0].is_leader);
    EXPECT_TRUE(m_members[1].is_leader);
    EXPECT_FALSE(m_members[2].is_leader);
}

TEST_F(ClusterMemberTest, resetAndBecomeCandidate)
{
    parse("0,a:b:c:d:e|1,a:b:c:d:e|2,a:b:c:d:e");
    /* prime dirty state */
    m_members[0].candidate_term_id = 3; m_members[0].vote = 1;
    m_members[1].candidate_term_id = 3; m_members[1].vote = 0;
    m_members[2].candidate_term_id = 2; m_members[2].vote = 1;

    aeron_cluster_members_reset(m_members, m_count);
    for (int i = 0; i < m_count; i++)
    {
        EXPECT_EQ(-1, m_members[i].vote);
        EXPECT_EQ(-1LL, m_members[i].candidate_term_id);
        EXPECT_FALSE(m_members[i].is_ballot_sent);
    }

    aeron_cluster_members_become_candidate(m_members, m_count, 5, 1);
    EXPECT_EQ(-1, m_members[0].vote);
    EXPECT_EQ(1,  m_members[1].vote);  /* self */
    EXPECT_EQ(5LL, m_members[1].candidate_term_id);
    EXPECT_TRUE(m_members[1].is_ballot_sent);
    EXPECT_EQ(-1, m_members[2].vote);
}

TEST_F(ClusterMemberTest, hasActiveQuorum)
{
    parse("0,a:b:c:d:e|1,a:b:c:d:e|2,a:b:c:d:e");
    const int64_t now_ns   = 1000000000LL;
    const int64_t timeout  = 500000000LL; /* 0.5 s */

    /* All members active */
    for (int i = 0; i < m_count; i++) { m_members[i].time_of_last_append_position_ns = now_ns; }
    EXPECT_TRUE(aeron_cluster_members_has_active_quorum(m_members, m_count, now_ns, timeout));

    /* Only 1 active — below quorum */
    m_members[1].time_of_last_append_position_ns = 0;
    m_members[2].time_of_last_append_position_ns = 0;
    EXPECT_FALSE(aeron_cluster_members_has_active_quorum(m_members, m_count, now_ns, timeout));
}

TEST_F(ClusterMemberTest, compareLog)
{
    parse("0,a:b:c:d:e|1,a:b:c:d:e");
    m_members[0].leadership_term_id = 2; m_members[0].log_position = 100;
    m_members[1].leadership_term_id = 2; m_members[1].log_position = 200;

    EXPECT_LT(aeron_cluster_member_compare_log(&m_members[0], &m_members[1]), 0);
    EXPECT_GT(aeron_cluster_member_compare_log(&m_members[1], &m_members[0]), 0);
    EXPECT_EQ(0, aeron_cluster_member_compare_log(&m_members[0], &m_members[0]));

    /* Higher term wins regardless of position */
    m_members[0].leadership_term_id = 3; m_members[0].log_position = 10;
    EXPECT_GT(aeron_cluster_member_compare_log(&m_members[0], &m_members[1]), 0);
}

TEST_F(ClusterMemberTest, areSameEndpoints)
{
    parse("0,i0:c0:l0:cu0:a0|1,i0:c0:l0:cu0:a0|2,i1:c0:l0:cu0:a0");
    /* 0 and 1 same endpoints */
    EXPECT_TRUE(aeron_cluster_member_are_same_endpoints(&m_members[0], &m_members[1]));
    /* 0 and 2 differ on ingress */
    EXPECT_FALSE(aeron_cluster_member_are_same_endpoints(&m_members[0], &m_members[2]));
}

TEST_F(ClusterMemberTest, collectIds)
{
    parse("0,a:b:c:d:e|2,a:b:c:d:e|5,a:b:c:d:e");
    int32_t ids[3];
    aeron_cluster_members_collect_ids(m_members, m_count, ids);
    EXPECT_EQ(0, ids[0]);
    EXPECT_EQ(2, ids[1]);
    EXPECT_EQ(5, ids[2]);
}

TEST_F(ClusterMemberTest, parseEndpointsFiveParts)
{
    aeron_cluster_member_t m{};
    ASSERT_EQ(0, aeron_cluster_member_parse_endpoints(&m, 3, "ing:20110,cons:20111,log:20113,catch:20114,arch:8010"));
    EXPECT_EQ(3, m.id);
    EXPECT_STREQ("ing:20110",  m.ingress_endpoint);
    EXPECT_STREQ("cons:20111", m.consensus_endpoint);
    EXPECT_STREQ("log:20113",  m.log_endpoint);
    EXPECT_STREQ("catch:20114",m.catchup_endpoint);
    EXPECT_STREQ("arch:8010",  m.archive_endpoint);
    /* Cleanup */
    free(m.ingress_endpoint); free(m.consensus_endpoint);
    free(m.log_endpoint); free(m.catchup_endpoint);
    free(m.archive_endpoint);
}

TEST_F(ClusterMemberTest, validateEndpointsMatchesOk)
{
    parse("7,ing:20110,cons:20111,log:20113,catch:20114,arch:8010");
    ASSERT_EQ(1, m_count);
    EXPECT_EQ(0, aeron_cluster_members_validate_endpoints(
        &m_members[0],
        "ing:20110,cons:20111,log:20113,catch:20114,arch:8010"));
}

TEST_F(ClusterMemberTest, validateEndpointsMismatchFails)
{
    parse("7,ing:20110,cons:20111,log:20113,catch:20114,arch:8010");
    ASSERT_EQ(1, m_count);
    EXPECT_NE(0, aeron_cluster_members_validate_endpoints(
        &m_members[0],
        "ing:99999,cons:20111,log:20113,catch:20114,arch:8010"));
}

TEST_F(ClusterMemberTest, isActiveWhenWithinTimeout)
{
    parse("0,a:b:c:d:e");
    ASSERT_EQ(1, m_count);
    m_members[0].time_of_last_append_position_ns = 1000LL;
    /* now_ns=1500, timeout=1000 → 1000+1000=2000 > 1500 → active */
    EXPECT_TRUE(aeron_cluster_member_is_active(&m_members[0], 1500LL, 1000LL));
}

TEST_F(ClusterMemberTest, isActiveExpiredWhenBeyondTimeout)
{
    parse("0,a:b:c:d:e");
    ASSERT_EQ(1, m_count);
    m_members[0].time_of_last_append_position_ns = 1000LL;
    /* now_ns=2001, timeout=1000 → 1000+1000=2000 < 2001 → not active */
    EXPECT_FALSE(aeron_cluster_member_is_active(&m_members[0], 2001LL, 1000LL));
}

TEST_F(ClusterMemberTest, isActiveExactlyAtDeadlineIsNotActive)
{
    parse("0,a:b:c:d:e");
    ASSERT_EQ(1, m_count);
    m_members[0].time_of_last_append_position_ns = 0LL;
    /* time_of_last + timeout == now_ns → NOT active (strict >) */
    EXPECT_FALSE(aeron_cluster_member_is_active(&m_members[0], 1000LL, 1000LL));
}

TEST_F(ClusterMemberTest, hasReachedPositionWhenAllConditionsMet)
{
    parse("0,a:b:c:d:e");
    ASSERT_EQ(1, m_count);
    m_members[0].time_of_last_append_position_ns = 0LL;
    m_members[0].leadership_term_id              = 3LL;
    m_members[0].log_position                   = 500LL;
    /* Active (0+2000 > 1000), correct term, position >= 500 */
    EXPECT_TRUE(aeron_cluster_member_has_reached_position(&m_members[0], 3LL, 500LL, 1000LL, 2000LL));
}

TEST_F(ClusterMemberTest, hasReachedPositionFailsIfExpired)
{
    parse("0,a:b:c:d:e");
    ASSERT_EQ(1, m_count);
    m_members[0].time_of_last_append_position_ns = 0LL;
    m_members[0].leadership_term_id              = 3LL;
    m_members[0].log_position                   = 500LL;
    /* Expired: 0+500 < 1000 */
    EXPECT_FALSE(aeron_cluster_member_has_reached_position(&m_members[0], 3LL, 500LL, 1000LL, 500LL));
}

TEST_F(ClusterMemberTest, hasReachedPositionFailsIfWrongTerm)
{
    parse("0,a:b:c:d:e");
    ASSERT_EQ(1, m_count);
    m_members[0].time_of_last_append_position_ns = 0LL;
    m_members[0].leadership_term_id              = 2LL;  /* wrong */
    m_members[0].log_position                   = 500LL;
    EXPECT_FALSE(aeron_cluster_member_has_reached_position(&m_members[0], 3LL, 500LL, 1000LL, 2000LL));
}

TEST_F(ClusterMemberTest, hasReachedPositionFailsIfBelowPosition)
{
    parse("0,a:b:c:d:e");
    ASSERT_EQ(1, m_count);
    m_members[0].time_of_last_append_position_ns = 0LL;
    m_members[0].leadership_term_id              = 3LL;
    m_members[0].log_position                   = 400LL; /* below required 500 */
    EXPECT_FALSE(aeron_cluster_member_has_reached_position(&m_members[0], 3LL, 500LL, 1000LL, 2000LL));
}

TEST_F(ClusterMemberTest, closePublicationSetsPublicationToNull)
{
    parse("0,a:b:c:d:e");
    ASSERT_EQ(1, m_count);
    /* publication is already NULL — close is a no-op and must not crash */
    ASSERT_EQ(nullptr, m_members[0].publication);
    aeron_cluster_member_close_publication(&m_members[0]);
    EXPECT_EQ(nullptr, m_members[0].publication);
}

TEST_F(ClusterMemberTest, hasReachedPositionWhenLogPositionGreaterThanRequired)
{
    /* Mirror Java: shouldReturnTrueIfLogPositionIsEqualOrGreaterThan */
    parse("0,a:b:c:d:e");
    m_members[0].time_of_last_append_position_ns = 0LL;
    m_members[0].leadership_term_id              = 5LL;
    m_members[0].log_position                   = 900LL;  /* > required 500 */
    EXPECT_TRUE(aeron_cluster_member_has_reached_position(&m_members[0], 5LL, 500LL, 1000LL, 2000LL));
}

TEST_F(ClusterMemberTest, hasReachedPositionFalseWhenLogPositionExactlyBelowRequired)
{
    /* log_position = required - 1 must return false */
    parse("0,a:b:c:d:e");
    m_members[0].time_of_last_append_position_ns = 0LL;
    m_members[0].leadership_term_id              = 5LL;
    m_members[0].log_position                   = 499LL;  /* one below */
    EXPECT_FALSE(aeron_cluster_member_has_reached_position(&m_members[0], 5LL, 500LL, 1000LL, 2000LL));
}

/* -----------------------------------------------------------------------
 * Parameterized isActive tests
 * ----------------------------------------------------------------------- */

struct IsActiveCase { int64_t ts; int64_t timeout; int64_t now; bool expected; };
static const IsActiveCase IS_ACTIVE_CASES[] = {
    /* active: ts + timeout > now */
    {1000, 1000, 1500, true},
    {0,    2000,  999, true},
    {5000, 5000, 9999, true},
    /* not active: ts + timeout <= now */
    {1000, 1000, 2001, false},
    {0,    1000, 1000, false},   /* == boundary: NOT active */
    {0,       1,    2, false},
};

class IsActiveParamTest : public ::testing::TestWithParam<IsActiveCase> {};
INSTANTIATE_TEST_SUITE_P(ClusterMember, IsActiveParamTest,
    ::testing::ValuesIn(IS_ACTIVE_CASES));

TEST_P(IsActiveParamTest, shouldCheckMemberIsActive)
{
    auto c = GetParam();
    aeron_cluster_member_t m{};
    m.time_of_last_append_position_ns = c.ts;
    EXPECT_EQ(c.expected, aeron_cluster_member_is_active(&m, c.now, c.timeout));
}
