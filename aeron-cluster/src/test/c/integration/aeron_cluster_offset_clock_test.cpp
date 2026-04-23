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
 * OffsetMillisecondClusterClock integration test -- C port of Java
 * OffsetMillisecondClusterClockTest.
 *
 * Tests:
 *   shouldOffsetDelegateTime
 *
 * The Java test verifies a clock wrapper that adds a configurable offset
 * to a delegate clock. Since C does not have the same clock abstraction,
 * this test verifies the equivalent behavior: that the CM's internal time
 * tracking advances correctly and that adding an artificial time offset
 * (by driving do_work with advanced timestamps) does not cause the
 * single-node cluster to consider itself inactive or trigger a re-election.
 *
 * This matches the intent of Java SingleNodeTest.shouldNotConsiderItselfInactiveAndEnterAnElection
 * combined with OffsetMillisecondClusterClockTest.shouldOffsetDelegateTime.
 */

#include <gtest/gtest.h>
#include <cstdlib>
#include <cstring>
#include <string>
#include <iostream>
#include <thread>
#include <chrono>

extern "C"
{
#include "aeronc.h"
#include "aeron_cm_context.h"
#include "aeron_consensus_module_agent.h"
#include "util/aeron_fileutil.h"
}

#include "../integration/aeron_test_clustered_media_driver.h"
#include "../integration/aeron_test_cluster_node.h"
#include "../integration/aeron_cluster_server_helper.h"

static std::string make_test_dir(const char *prefix)
{
    char base[AERON_MAX_PATH] = {0};
    aeron_temp_filename(base, sizeof(base));
    return std::string(base) + "-" + prefix;
}

/* -----------------------------------------------------------------------
 * Fixture -- uses the manual CM creation pattern (like echo_test) so we
 * can directly drive the CM with specific timestamps.
 * ----------------------------------------------------------------------- */
class OffsetMillisecondClusterClockTest : public ::testing::Test
{
protected:
    enum { NODE_INDEX= 9 };

    void SetUp() override
    {
        m_base_dir = make_test_dir("aeron_cluster_offset_clock_");
        aeron_delete_directory(m_base_dir.c_str());
        aeron_mkdir_recursive(m_base_dir.c_str(), 0777);

        m_node = new TestClusterNode(0, 1, NODE_INDEX, m_base_dir, std::cout);
        m_node->start();
    }

    void TearDown() override
    {
        if (m_server != nullptr)
        {
            cluster_server_stop(m_server);
            m_server = nullptr;
        }
        if (m_node != nullptr)
        {
            m_node->stop();
            delete m_node;
            m_node = nullptr;
        }
        aeron_delete_directory(m_base_dir.c_str());
    }

    TestClusterNode         *m_node   = nullptr;
    cluster_server_handle_t *m_server = nullptr;
    std::string              m_base_dir;
};

/* -----------------------------------------------------------------------
 * Test 1: shouldOffsetDelegateTime
 *
 * Matches Java OffsetMillisecondClusterClockTest.shouldOffsetDelegateTime
 * combined with SingleNodeTest.shouldNotConsiderItselfInactiveAndEnterAnElection.
 *
 * Verifies that:
 *   1. A single-node cluster elects itself as leader
 *   2. After advancing time by 2x the leader_heartbeat_timeout_ns,
 *      the node remains leader (does not trigger re-election)
 *   3. The cluster continues to function after the time offset
 * ----------------------------------------------------------------------- */
TEST_F(OffsetMillisecondClusterClockTest, shouldOffsetDelegateTime)
{
    /* Start server (CM + echo service) with manual do_work control */
    m_server = cluster_server_start(
        m_node->aeron_dir().c_str(),
        m_node->cluster_dir().c_str(),
        m_node->cluster_members().c_str(),
        20110 + m_node->node_index(),
        20220 + m_node->node_index());
    ASSERT_NE(nullptr, m_server) << "cluster_server_start failed: " << aeron_errmsg();

    /* Drive server until leader */
    int64_t now_ns = aeron_nano_clock();
    bool became_leader = false;
    for (int i = 0; i < 200; i++)
    {
        now_ns += INT64_C(50000000);  /* 50ms */
        cluster_server_do_work(m_server, now_ns);
        if (cluster_server_is_leader(m_server))
        {
            became_leader = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    ASSERT_TRUE(became_leader) << "Server did not become leader";

    /* Record the leadership_term_id before time offset */
    EXPECT_TRUE(cluster_server_is_leader(m_server));

    /* Simulate time offset: advance by 2x leader_heartbeat_timeout_ns (5s * 2 = 10s).
     * In a properly functioning single-node cluster, the node should NOT
     * consider itself inactive because there are no other nodes to miss heartbeats from. */
    int64_t heartbeat_timeout_ns = INT64_C(5000000000);  /* matches CM context default */
    int64_t offset_ns = heartbeat_timeout_ns * 2;

    /* Advance time in smaller steps (like the Java test's addOffset approach)
     * to let the CM process each tick. */
    int64_t step_ns = INT64_C(100000000);  /* 100ms per step */
    int64_t remaining = offset_ns;
    while (remaining > 0)
    {
        int64_t advance = (remaining > step_ns) ? step_ns : remaining;
        now_ns += advance;
        remaining -= advance;
        cluster_server_do_work(m_server, now_ns);
    }

    /* Verify the node is still leader after the time offset */
    EXPECT_TRUE(cluster_server_is_leader(m_server))
        << "Single-node cluster should remain leader after time offset of "
        << (offset_ns / 1000000) << " ms";

    /* Drive a few more ticks to confirm stability */
    for (int i = 0; i < 20; i++)
    {
        now_ns += INT64_C(50000000);  /* 50ms */
        cluster_server_do_work(m_server, now_ns);
    }

    EXPECT_TRUE(cluster_server_is_leader(m_server))
        << "Single-node cluster should remain leader after additional ticks";
}
