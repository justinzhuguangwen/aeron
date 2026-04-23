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
 * Phase 2b harness tests: verify TestCluster boots 3 nodes with each one
 * running its own clustered service on a dedicated background thread, and
 * that election + failover still work with the service in the picture.
 *
 * If these fail, the messaging upgrades that will be built on top of Phase 2b
 * (client offer, egress polling, echo round-trips) won't work either, so
 * these are the canary for the whole Phase 2 path.
 */

#include <gtest/gtest.h>
#include <chrono>
#include <thread>

#include "aeron_test_cluster.h"

class TestClusterSvcHarnessTest : public ::testing::Test
{
};

/* 3 nodes, services on — elect leader organically, all services remain alive. */
TEST_F(TestClusterSvcHarnessTest, shouldElectLeaderWithServiceOn)
{
    TestCluster cluster(3, 200, "svc_harness_elect");
    ASSERT_EQ(0, cluster.start_all(-1, /*with_service=*/true));

    const int leader = cluster.await_leader();
    ASSERT_GE(leader, 0) << "no leader elected (service on)";

    for (int i = 0; i < cluster.node_count(); ++i)
    {
        EXPECT_NE(nullptr, cluster.svc_agent(i)) << "svc agent " << i << " missing";
    }
}

/* Service on — kill leader, remaining nodes elect a new one. */
TEST_F(TestClusterSvcHarnessTest, shouldFailoverWithServiceOn)
{
    TestCluster cluster(3, 204, "svc_harness_failover");
    ASSERT_EQ(0, cluster.start_all(-1, /*with_service=*/true));

    const int first = cluster.await_leader();
    ASSERT_GE(first, 0);

    const int stopped = cluster.stop_leader();
    EXPECT_EQ(first, stopped);

    const int next = cluster.await_leader_role(6000);
    ASSERT_GE(next, 0) << "no new leader after failover (service on)";
    EXPECT_NE(stopped, next);
}

/* Service on, **single-node** cluster — client connects, offers 1 message,
 * echo service replies. This is the simplest end-to-end path for Phase 2b:
 * no leader election delays, no cross-node log replication, just one node
 * running CM + service + archive. If this passes, the Phase 2b plumbing
 * itself is correct; scaling to 3-node then only has to chase the
 * `invalid recordingId=-1` issue that currently blocks the 3-node echo test. */
TEST_F(TestClusterSvcHarnessTest, shouldEchoSingleNodeEndToEnd)
{
    TestCluster cluster(1, 210, "svc_harness_echo_1n");
    ASSERT_EQ(0, cluster.start_all(0, /*with_service=*/true));

    const int leader = cluster.await_leader();
    ASSERT_GE(leader, 0);

    ASSERT_EQ(0, cluster.connect_client()) << "client connect failed: " << aeron_errmsg();

    static const char MSG[] = "hello-phase-2b-1n";
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

/* Service on — 3-node client connects, offers 1 message, echo service replies.
 * Exercises the full REDIRECT path when the client happens to target a
 * follower first; mirrors Java's 3-node AeronCluster connect + echo smoke. */
TEST_F(TestClusterSvcHarnessTest, shouldEchoSingleMessage)
{
    TestCluster cluster(3, 300, "svc_harness_echo");
    ASSERT_EQ(0, cluster.start_all(-1, /*with_service=*/true));

    const int leader = cluster.await_leader();
    ASSERT_GE(leader, 0);

    ASSERT_EQ(0, cluster.connect_client()) << "client connect failed: " << aeron_errmsg();

    static const char MSG[] = "hello-phase-2b";
    bool sent = false;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (!sent && std::chrono::steady_clock::now() < deadline)
    {
        if (cluster.offer(MSG, sizeof(MSG) - 1) > 0) { sent = true; break; }
        cluster.poll_egress_once();
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    ASSERT_TRUE(sent) << "offer failed within 5s";

    EXPECT_GE(cluster.await_response_count(1, 10000), 1)
        << "no echo response within 10s";
}
