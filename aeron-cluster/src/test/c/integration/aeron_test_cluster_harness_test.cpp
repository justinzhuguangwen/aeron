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
 * Tests for the multi-node TestCluster harness itself.
 *
 * These are sanity checks that cover the three things the harness is supposed
 * to make easy (election, leader failover, node restart) so any regression in
 * the harness surfaces here before it breaks the downstream system_test suite
 * that will depend on it.
 */

#include <gtest/gtest.h>

#include "aeron_test_cluster.h"

class TestClusterHarnessTest : public ::testing::Test
{
};

/* Start 3 nodes, organic election picks a leader, remaining two are followers. */
TEST_F(TestClusterHarnessTest, shouldElectLeaderOrganically)
{
    TestCluster cluster(3, 24, "harness_elect");
    ASSERT_EQ(0, cluster.start_all(-1));

    const int leader = cluster.await_leader();
    ASSERT_GE(leader, 0) << "no leader elected";

    int followers = 0;
    for (int i = 0; i < cluster.node_count(); ++i)
    {
        if (i == leader)
        {
            EXPECT_TRUE(cluster.is_leader(i));
        }
        else
        {
            EXPECT_TRUE(cluster.is_follower(i)) << "node " << i;
            ++followers;
        }
    }
    EXPECT_EQ(2, followers);
}

/* Start 3 nodes, kill the leader, a different node takes over. */
TEST_F(TestClusterHarnessTest, shouldFailoverWhenLeaderStopped)
{
    TestCluster cluster(3, 27, "harness_failover");
    ASSERT_EQ(0, cluster.start_all(-1));

    const int first = cluster.await_leader();
    ASSERT_GE(first, 0);

    const int stopped = cluster.stop_leader();
    EXPECT_EQ(first, stopped);

    const int next = cluster.await_leader();
    ASSERT_GE(next, 0) << "no new leader after failover";
    EXPECT_NE(stopped, next);
}

/* Killing and restarting a node returns the cluster to a 3-node quorum. */
TEST_F(TestClusterHarnessTest, shouldRestartStoppedNode)
{
    TestCluster cluster(3, 30, "harness_restart");
    ASSERT_EQ(0, cluster.start_all(-1));

    const int leader = cluster.await_leader();
    ASSERT_GE(leader, 0);

    /* Pick a follower and bounce it */
    int follower = -1;
    for (int i = 0; i < cluster.node_count(); ++i)
    {
        if (i != leader) { follower = i; break; }
    }
    ASSERT_GE(follower, 0);

    cluster.stop_node(follower);
    EXPECT_EQ(nullptr, cluster.cm_agent(follower));

    ASSERT_EQ(0, cluster.restart_node(follower));
    EXPECT_NE(nullptr, cluster.cm_agent(follower));

    /* After restart, the cluster should settle back to a leader and the
     * restarted node should rejoin as follower. Which node leads after the
     * bounce isn't guaranteed — the rejoining node can briefly trigger a
     * re-election — so just assert a leader role exists. */
    const bool rejoined = cluster.await(
        [&]() { return cluster.is_follower(follower); },
        500);
    EXPECT_TRUE(rejoined) << "restarted node did not rejoin as follower";
    const int stable = cluster.await_leader_role(500);
    EXPECT_GE(stable, 0) << "cluster did not return to a leader after follower rejoin";
}
