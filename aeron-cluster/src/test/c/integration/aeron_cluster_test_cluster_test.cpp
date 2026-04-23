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
 * TestCluster infrastructure integration test -- C port of Java TestClusterTest.
 *
 * Tests:
 *   testCustomAeronDirectory
 *
 * Verifies that the TestClusteredMediaDriver infrastructure correctly sets up
 * aeron_dir paths and that each node/client gets a unique directory rooted
 * under the provided base directory.
 */

#include <gtest/gtest.h>
#include <cstdlib>
#include <cstring>
#include <string>
#include <iostream>
#include <thread>
#include <chrono>
#include <set>

extern "C"
{
#include "aeronc.h"
#include "aeron_common.h"
#include "util/aeron_fileutil.h"
}

#include "../integration/aeron_test_clustered_media_driver.h"
#include "../integration/aeron_cluster_server_helper.h"

static std::string make_test_dir(const char *prefix)
{
    char base[AERON_MAX_PATH] = {0};
    aeron_temp_filename(base, sizeof(base));
    return std::string(base) + "-" + prefix;
}

/* -----------------------------------------------------------------------
 * Fixture
 * ----------------------------------------------------------------------- */
class TestClusterTest : public ::testing::Test
{
protected:
    enum { NODE_INDEX= 8 };

    void SetUp() override
    {
        m_base_dir = make_test_dir("aeron_cluster_testcluster_");
        aeron_delete_directory(m_base_dir.c_str());
    }

    void TearDown() override
    {
        if (m_cmd != nullptr)
        {
            m_cmd->close();
            delete m_cmd;
            m_cmd = nullptr;
        }
        aeron_delete_directory(m_base_dir.c_str());
    }

    TestClusteredMediaDriver *m_cmd = nullptr;
    std::string               m_base_dir;
};

/* -----------------------------------------------------------------------
 * Test 1: testCustomAeronDirectory
 * Matches Java TestClusterTest.testCustomAeronDirectory
 *
 * Verifies that:
 *   - The TestClusteredMediaDriver aeron_dir is rooted under the base_dir
 *   - The aeron_dir, archive_dir, and cluster_dir are all distinct
 *   - The driver launches successfully with the custom directory
 * ----------------------------------------------------------------------- */
TEST_F(TestClusterTest, testCustomAeronDirectory)
{
    m_cmd = new TestClusteredMediaDriver(0, 1, NODE_INDEX, m_base_dir, std::cout);

    /* Verify directories are rooted under base_dir */
    EXPECT_NE(std::string::npos, m_cmd->aeron_dir().find(m_base_dir))
        << "aeron_dir should be under base_dir";
    EXPECT_NE(std::string::npos, m_cmd->archive_dir().find(m_base_dir))
        << "archive_dir should be under base_dir";
    EXPECT_NE(std::string::npos, m_cmd->cluster_dir().find(m_base_dir))
        << "cluster_dir should be under base_dir";

    /* Verify all directories are distinct */
    std::set<std::string> dirs;
    EXPECT_TRUE(dirs.insert(m_cmd->aeron_dir()).second) << "aeron_dir should be unique";
    EXPECT_TRUE(dirs.insert(m_cmd->archive_dir()).second) << "archive_dir should be unique";
    EXPECT_TRUE(dirs.insert(m_cmd->cluster_dir()).second) << "cluster_dir should be unique";

    /* Verify driver launches successfully */
    ASSERT_EQ(0, m_cmd->launch()) << "ClusteredMediaDriver launch failed";

    /* Verify becomes leader (single-node cluster) */
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (!m_cmd->is_leader() && std::chrono::steady_clock::now() < deadline)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    EXPECT_TRUE(m_cmd->is_leader()) << "Single-node cluster should elect itself as leader";

    /* Verify ingress_endpoints contains node_index */
    EXPECT_NE(std::string::npos,
        m_cmd->ingress_endpoints().find(std::to_string(NODE_INDEX)))
        << "ingress_endpoints should reference node_index " << NODE_INDEX;
}
