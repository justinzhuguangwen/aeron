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
 * Multi-node ClusterSystemTest cases upgraded to use the real 3-node
 * TestCluster harness instead of the 5-line single-node smoke version that
 * still lives in aeron_cluster_system_test.cpp.
 *
 * The same-named tests in aeron_cluster_system_test.cpp remain as fallback
 * smoke tests; these are the ones that actually exercise multi-node behaviour
 * (leader failover, follower bounce, quorum loss, two-node restart).
 *
 * Phase 1 coverage: election + role transitions. Messaging (offer / egress)
 * is not yet wired because the harness does not yet include a per-node
 * service — that lands in Phase 2.
 */

#include <gtest/gtest.h>

#include "aeron_test_cluster.h"

extern "C"
{
#include "aeron_archive.h"
}

class ClusterSystemMultiTest : public ::testing::Test
{
};

/* -----------------------------------------------------------------------
 * Java: shouldEnterElectionWhenRecordingStopsUnexpectedlyOnLeaderOfSingleNodeCluster
 *
 * End-to-end path: leader CM picks up its log recording_id from the
 * archive's START RecordingSignal; we call try_stop_recording_by_identity
 * on the CM's own archive client, which causes the archive server to
 * close the recording session and emit STOP; the CM's consumer flips
 * log_recording_stopped_unexpectedly, which slow_tick_work drains into
 * enterElection; single-node cluster immediately re-elects itself with
 * leadership_term_id advanced.
 *
 * This test is the TDD driver for the C archive server's recording
 * session lifecycle + START/STOP signal emission (P1 of the server-side
 * gap list). See test-gap-report.md §5.5.
 * ----------------------------------------------------------------------- */
TEST_F(ClusterSystemMultiTest, shouldEnterElectionWhenRecordingStopsUnexpectedlyOnLeaderOfSingleNodeCluster)
{
    TestCluster cluster(1, 123, "sysmulti_recording_stop_1n");
    ASSERT_EQ(0, cluster.start_all(0, /*with_service=*/true));

    const int leader = cluster.await_leader();
    ASSERT_GE(leader, 0);

    aeron_consensus_module_agent_t *agent = cluster.cm_agent(leader);
    ASSERT_NE(nullptr, agent);

    /* CM picks up log_recording_id from the archive START signal — proves
     * the server creates a recording_session + catalog entry + emits START
     * when the log publisher's image appears on the archive subscription. */
    const bool got_recording = cluster.await(
        [&]() { return agent->log_recording_id >= 0; }, 500);
    ASSERT_TRUE(got_recording)
        << "CM never saw START signal for its log recording "
        << "(sub=" << agent->log_subscription_id << " rec=" << agent->log_recording_id << ")";

    const int64_t recording_id = agent->log_recording_id;
    const int64_t initial_term_id = agent->leadership_term_id;

    /* Ask the archive to stop the log recording via the CM's own archive
     * client. Server must: find recording_session by recording_id, abort it,
     * and emit STOP signal back on this control session. */
    bool stopped = false;
    ASSERT_EQ(0, aeron_archive_try_stop_recording_by_identity(
        &stopped, agent->archive, recording_id)) << aeron_errmsg();
    EXPECT_TRUE(stopped);

    /* CM's consumer should observe STOP → raise log_recording_stopped_unexpectedly
     * → slow_tick_work drains it into enterElection → single-node re-elects. */
    const bool new_term = cluster.await(
        [&]() { return agent->leadership_term_id > initial_term_id; }, 2000);
    EXPECT_TRUE(new_term)
        << "leader did not enter a new term after recording stopped "
        << "(term=" << agent->leadership_term_id
        << " initial=" << initial_term_id << ")";

    const int new_leader = cluster.await_leader(2000);
    EXPECT_EQ(leader, new_leader);
}

/* -----------------------------------------------------------------------
 * Java: shouldCallOnRoleChangeOnBecomingLeaderSingleNodeCluster
 * Start a single-node cluster with echo service; verify the service observed
 * the LEADER role via its on_role_change callback after election.
 * Uses Phase 2b (service on its own wall-clock thread).
 * ----------------------------------------------------------------------- */
TEST_F(ClusterSystemMultiTest, shouldCallOnRoleChangeOnBecomingLeaderSingleNodeCluster)
{
    TestCluster cluster(1, 117, "sysmulti_role_change_1n");
    ASSERT_EQ(0, cluster.start_all(0, /*with_service=*/true));

    const int leader = cluster.await_leader();
    ASSERT_GE(leader, 0);

    /* Role change callback fires on its own thread — give it up to 2 s wall
     * to propagate from CM to the service after election closes. */
    const bool got_leader = cluster.await(
        [&]() { return cluster.service_last_role(0) == AERON_CLUSTER_ROLE_LEADER; },
        200);
    EXPECT_TRUE(got_leader)
        << "service never observed LEADER role (last_role=" << cluster.service_last_role(0) << ")";
}

/* -----------------------------------------------------------------------
 * Java: shouldSetClientName
 * Simplified: start a single-node cluster with service; verify the client
 * connects, offers once, gets the echo. This proves Phase 2b end-to-end
 * messaging works on single-node; the Java test's additional assertion on
 * client name labels requires per-agent `agentRoleName()` access which isn't
 * exposed by the C service API.
 * ----------------------------------------------------------------------- */
TEST_F(ClusterSystemMultiTest, shouldSetClientName)
{
    TestCluster cluster(1, 120, "sysmulti_set_client_name");
    ASSERT_EQ(0, cluster.start_all(0, /*with_service=*/true));

    const int leader = cluster.await_leader();
    ASSERT_GE(leader, 0);

    ASSERT_EQ(0, cluster.connect_client()) << "client connect failed: " << aeron_errmsg();

    static const char MSG[] = "set-client-name-probe";
    bool sent = false;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (!sent && std::chrono::steady_clock::now() < deadline)
    {
        if (cluster.offer(MSG, sizeof(MSG) - 1) > 0) { sent = true; break; }
        cluster.poll_egress_once();
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    ASSERT_TRUE(sent) << "offer failed within 10s";

    EXPECT_GE(cluster.await_response_count(1, 10000), 1)
        << "no echo response within 10s";
}

/* -----------------------------------------------------------------------
 * Java: shouldStopFollowerAndRestartFollower
 * Verifies a 3-node cluster keeps its leader when a follower bounces.
 * ----------------------------------------------------------------------- */
TEST_F(ClusterSystemMultiTest, shouldStopFollowerAndRestartFollower)
{
    TestCluster cluster(3, 33, "sysmulti_follower_bounce");
    ASSERT_EQ(0, cluster.start_all(-1));

    const int leader = cluster.await_leader();
    ASSERT_GE(leader, 0);

    int follower = (leader == 0) ? 1 : 0;
    cluster.stop_node(follower);
    EXPECT_TRUE(cluster.is_leader(leader))
        << "leader should keep leading while only one follower is down";

    ASSERT_EQ(0, cluster.restart_node(follower));
    const bool rejoined = cluster.await(
        [&]() { return cluster.is_follower(follower); }, 500);
    EXPECT_TRUE(rejoined) << "bounced follower did not rejoin";

    /* After the rejoin the cluster should settle back into a stable leader;
     * which node ends up leading is not guaranteed if the rejoin briefly
     * re-opens an election, so just assert that some leader role exists. */
    const int stable = cluster.await_leader_role(500);
    EXPECT_GE(stable, 0) << "cluster did not return to a leader after follower rejoin";
}

/* -----------------------------------------------------------------------
 * Java: shouldElectNewLeaderAfterGracefulLeaderClose
 * A gracefully-closed leader triggers a clean re-election to a different node.
 * ----------------------------------------------------------------------- */
TEST_F(ClusterSystemMultiTest, shouldElectNewLeaderAfterGracefulLeaderClose)
{
    TestCluster cluster(3, 45, "sysmulti_leader_close");
    ASSERT_EQ(0, cluster.start_all(-1));

    const int first = cluster.await_leader();
    ASSERT_GE(first, 0);

    const int stopped = cluster.stop_leader();
    EXPECT_EQ(first, stopped);

    const int next = cluster.await_leader();
    ASSERT_GE(next, 0) << "no new leader after graceful leader close";
    EXPECT_NE(stopped, next);
}

/* -----------------------------------------------------------------------
 * Java: shouldTolerateMultipleLeaderFailures
 * Kill leader, elect new, kill the second leader after restoring the first;
 * cluster must still be electable with 2 nodes up at each election step.
 * ----------------------------------------------------------------------- */
TEST_F(ClusterSystemMultiTest, shouldTolerateMultipleLeaderFailures)
{
    TestCluster cluster(3, 57, "sysmulti_multi_failure");
    ASSERT_EQ(0, cluster.start_all(-1));

    const int first = cluster.await_leader();
    ASSERT_GE(first, 0);

    /* First failure: kill leader, re-elect */
    const int stopped1 = cluster.stop_leader();
    const int second = cluster.await_leader();
    ASSERT_GE(second, 0) << "first failover failed";
    EXPECT_NE(stopped1, second);

    /* Restore first so we keep 2+ nodes alive for the next failover */
    ASSERT_EQ(0, cluster.restart_node(stopped1));
    cluster.await([&]() { return cluster.is_follower(stopped1); }, 500);

    /* Second failure: kill the new leader, re-elect again */
    const int stopped2 = cluster.stop_leader();
    EXPECT_EQ(second, stopped2);
    const int third = cluster.await_leader();
    ASSERT_GE(third, 0) << "second failover failed";
    EXPECT_NE(stopped2, third);
}

/* -----------------------------------------------------------------------
 * Java: shouldRecoverAfterTwoLeaderNodesFailAndComeBackUpAtSameTime
 * Kill two nodes (including leader), cluster loses quorum, then restart both
 * and verify a leader is re-elected.
 * ----------------------------------------------------------------------- */
TEST_F(ClusterSystemMultiTest, shouldRecoverAfterTwoLeaderNodesFailAndComeBackUpAtSameTime)
{
    TestCluster cluster(3, 69, "sysmulti_two_node_recover");
    ASSERT_EQ(0, cluster.start_all(-1));

    const int leader = cluster.await_leader();
    ASSERT_GE(leader, 0);

    /* Kill leader + one follower → quorum lost */
    const int stopped_leader = cluster.stop_leader();
    int also_stopped = -1;
    for (int i = 0; i < cluster.node_count(); ++i)
    {
        if (i != stopped_leader && cluster.cm_agent(i) != nullptr)
        {
            also_stopped = i;
            cluster.stop_node(i);
            break;
        }
    }
    ASSERT_GE(also_stopped, 0);

    /* Bring both back up — cluster should elect a new leader. */
    ASSERT_EQ(0, cluster.restart_node(stopped_leader));
    ASSERT_EQ(0, cluster.restart_node(also_stopped));

    const int recovered = cluster.await_leader();
    ASSERT_GE(recovered, 0) << "no leader after two-node restart";
}

/* -----------------------------------------------------------------------
 * Java: shouldStopLeaderAndRestartAsFollower
 * After a leader is killed and a new one is elected, restarting the old
 * leader brings it back as a follower — the new leader keeps leading.
 * ----------------------------------------------------------------------- */
TEST_F(ClusterSystemMultiTest, shouldStopLeaderAndRestartAsFollower)
{
    TestCluster cluster(3, 81, "sysmulti_leader_restart_as_follower");
    ASSERT_EQ(0, cluster.start_all(-1));

    const int first = cluster.await_leader();
    ASSERT_GE(first, 0);

    const int stopped = cluster.stop_leader();
    const int new_leader = cluster.await_leader();
    ASSERT_GE(new_leader, 0);
    EXPECT_NE(stopped, new_leader);

    /* Restart old leader — it should rejoin as follower, not regain leadership */
    ASSERT_EQ(0, cluster.restart_node(stopped));
    const bool rejoined = cluster.await(
        [&]() { return cluster.is_follower(stopped); }, 500);
    EXPECT_TRUE(rejoined) << "old leader did not rejoin as follower";
    EXPECT_TRUE(cluster.is_leader(new_leader))
        << "new leader should keep leading after old leader rejoins";
}

/* -----------------------------------------------------------------------
 * Java: shouldLoseLeadershipWhenNoActiveQuorumOfFollowers
 * Kill both followers so the leader can no longer reach quorum; the leader
 * must step down out of LEADER role.
 * ----------------------------------------------------------------------- */
TEST_F(ClusterSystemMultiTest, shouldLoseLeadershipWhenNoActiveQuorumOfFollowers)
{
    TestCluster cluster(3, 93, "sysmulti_quorum_loss");
    ASSERT_EQ(0, cluster.start_all(-1));

    const int leader = cluster.await_leader();
    ASSERT_GE(leader, 0);

    int killed = 0;
    for (int i = 0; i < cluster.node_count(); ++i)
    {
        if (i != leader)
        {
            cluster.stop_node(i);
            ++killed;
        }
    }
    EXPECT_EQ(2, killed);

    const bool stepped_down = cluster.await(
        [&]() { return !cluster.is_leader(leader); }, 500);
    EXPECT_TRUE(stepped_down)
        << "leader should lose LEADER role once the other two nodes are gone";
}

/* -----------------------------------------------------------------------
 * Java: shouldElectSameLeaderAfterLoosingQuorum
 * After quorum loss the leader steps down; when one follower comes back,
 * the original leader's log is more up-to-date, so it gets re-elected.
 * ----------------------------------------------------------------------- */
TEST_F(ClusterSystemMultiTest, shouldElectSameLeaderAfterLoosingQuorum)
{
    TestCluster cluster(3, 105, "sysmulti_same_leader");
    ASSERT_EQ(0, cluster.start_all(-1));

    const int original_leader = cluster.await_leader();
    ASSERT_GE(original_leader, 0);

    int first_follower_killed = -1;
    for (int i = 0; i < cluster.node_count(); ++i)
    {
        if (i != original_leader)
        {
            if (first_follower_killed < 0) { first_follower_killed = i; }
            cluster.stop_node(i);
        }
    }

    /* Leader steps down while no quorum is reachable */
    cluster.await([&]() { return !cluster.is_leader(original_leader); }, 500);

    /* Restore one follower → quorum reached again */
    ASSERT_EQ(0, cluster.restart_node(first_follower_killed));

    /* Use await_leader_role (permissive) because the restarted follower's
     * catch-up keeps the election open for a while even after the leader has
     * resumed LEADER role. */
    const int re_elected = cluster.await_leader_role();
    ASSERT_GE(re_elected, 0) << "no leader after quorum restored";
    /* The original leader has the freshest log, so it should win re-election. */
    EXPECT_EQ(original_leader, re_elected)
        << "original leader should be re-elected since it has the most up-to-date log";
}
