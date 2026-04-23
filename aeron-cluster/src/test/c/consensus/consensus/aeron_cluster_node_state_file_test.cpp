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
#include <cstdio>
#include <string>
#include <sys/stat.h>

extern "C"
{
#include "aeron_common.h"
#include "util/aeron_fileutil.h"
#include "aeron_cluster_node_state_file.h"
}

#define AERON_NULL_VALUE (-1LL)

static std::string make_test_dir(const char *prefix)
{
    char base[AERON_MAX_PATH] = {0};
    aeron_temp_filename(base, sizeof(base));
    return std::string(base) + "-" + prefix;
}

/* -----------------------------------------------------------------------
 * Constants (mirror Java NodeStateFileTest)
 * ----------------------------------------------------------------------- */
static constexpr int64_t V_1_42_X_CANDIDATE_TERM_ID= 982374;
static constexpr int64_t V_1_42_X_LOG_POSITION = 89723648762342LL;
static constexpr int64_t V_1_42_X_TIMESTAMP_MS = 9878967687234LL;

/* -----------------------------------------------------------------------
 * NodeStateFile tests
 * ----------------------------------------------------------------------- */
class NodeStateFileTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        m_dir = make_test_dir("aeron_cluster_test_node_state_file_");
        aeron_delete_directory(m_dir.c_str());
        aeron_mkdir_recursive(m_dir.c_str(), 0777);
    }

    void TearDown() override
    {
        aeron_delete_directory(m_dir.c_str());
    }

    enum { SYNC_LEVEL= 1 };

    std::string m_dir;
};

TEST_F(NodeStateFileTest, shouldFailIfCreateNewFalseAndFileDoesNotExist)
{
    aeron_cluster_node_state_file_t *nsf = nullptr;
    EXPECT_EQ(-1, aeron_cluster_node_state_file_open(&nsf, m_dir.c_str(), false, SYNC_LEVEL));
    EXPECT_EQ(nullptr, nsf);
}

TEST_F(NodeStateFileTest, shouldCreateIfCreateNewTrueAndFileDoesNotExist)
{
    aeron_cluster_node_state_file_t *nsf = nullptr;
    ASSERT_EQ(0, aeron_cluster_node_state_file_open(&nsf, m_dir.c_str(), true, SYNC_LEVEL));
    ASSERT_NE(nullptr, nsf);

    std::string expected_path = m_dir + "/" + AERON_CLUSTER_NODE_STATE_FILE_FILENAME;
    struct stat st;
    EXPECT_EQ(0, stat(expected_path.c_str(), &st));

    aeron_cluster_node_state_file_close(nsf);
}

TEST_F(NodeStateFileTest, shouldHaveNullCandidateTermIdOnInitialCreation)
{
    aeron_cluster_node_state_file_t *nsf = nullptr;
    ASSERT_EQ(0, aeron_cluster_node_state_file_open(&nsf, m_dir.c_str(), true, SYNC_LEVEL));
    ASSERT_NE(nullptr, nsf);

    EXPECT_EQ(AERON_NULL_VALUE, aeron_cluster_node_state_file_candidate_term_id(nsf));

    aeron_cluster_node_state_file_close(nsf);
}

TEST_F(NodeStateFileTest, shouldPersistCandidateTermId)
{
    const int64_t candidate_term_id= 832234;
    const int64_t timestamp_ms= 324234;
    const int64_t log_position= 8923423;

    {
        aeron_cluster_node_state_file_t *nsf = nullptr;
        ASSERT_EQ(0, aeron_cluster_node_state_file_open(&nsf, m_dir.c_str(), true, SYNC_LEVEL));
        ASSERT_NE(nullptr, nsf);

        aeron_cluster_node_state_file_update_candidate_term_id(nsf, candidate_term_id, log_position, timestamp_ms);

        aeron_cluster_node_state_file_close(nsf);
    }

    {
        aeron_cluster_node_state_file_t *nsf = nullptr;
        ASSERT_EQ(0, aeron_cluster_node_state_file_open(&nsf, m_dir.c_str(), false, SYNC_LEVEL));
        ASSERT_NE(nullptr, nsf);

        EXPECT_EQ(candidate_term_id, aeron_cluster_node_state_file_candidate_term_id(nsf));
        EXPECT_EQ(timestamp_ms, aeron_cluster_node_state_file_timestamp(nsf));
        EXPECT_EQ(log_position, aeron_cluster_node_state_file_log_position(nsf));

        aeron_cluster_node_state_file_close(nsf);
    }
}

TEST_F(NodeStateFileTest, shouldThrowIfVersionMismatch)
{
    {
        aeron_cluster_node_state_file_t *nsf = nullptr;
        ASSERT_EQ(0, aeron_cluster_node_state_file_open(&nsf, m_dir.c_str(), true, SYNC_LEVEL));
        ASSERT_NE(nullptr, nsf);
        aeron_cluster_node_state_file_close(nsf);
    }

    /* Corrupt the version field in the file */
    {
        std::string path = m_dir + "/" + AERON_CLUSTER_NODE_STATE_FILE_FILENAME;
        FILE *fp = fopen(path.c_str(), "r+b");
        ASSERT_NE(nullptr, fp);
        int32_t bad_version = AERON_CLUSTER_NODE_STATE_FILE_VERSION + 1;
        fseek(fp, AERON_CLUSTER_NSF_VERSION_OFFSET, SEEK_SET);
        fwrite(&bad_version, sizeof(bad_version), 1, fp);
        fclose(fp);
    }

    {
        aeron_cluster_node_state_file_t *nsf = nullptr;
        EXPECT_EQ(-1, aeron_cluster_node_state_file_open(&nsf, m_dir.c_str(), false, SYNC_LEVEL));
        EXPECT_EQ(nullptr, nsf);
    }
}

TEST_F(NodeStateFileTest, shouldProposeNewMaxTermId)
{
    int64_t next_candidate_term_id;

    {
        aeron_cluster_node_state_file_t *nsf = nullptr;
        ASSERT_EQ(0, aeron_cluster_node_state_file_open(&nsf, m_dir.c_str(), true, SYNC_LEVEL));
        ASSERT_NE(nullptr, nsf);

        aeron_cluster_node_state_file_update_candidate_term_id(nsf, 5, 10, 10);

        next_candidate_term_id = aeron_cluster_node_state_file_candidate_term_id(nsf) + 1;
        EXPECT_EQ(
            next_candidate_term_id,
            aeron_cluster_node_state_file_propose_max_candidate_term_id(nsf, next_candidate_term_id, 20, 20));

        const int64_t too_low_candidate_term_id = aeron_cluster_node_state_file_candidate_term_id(nsf) - 1;
        EXPECT_EQ(
            next_candidate_term_id,
            aeron_cluster_node_state_file_propose_max_candidate_term_id(nsf, too_low_candidate_term_id, 30, 30));

        aeron_cluster_node_state_file_close(nsf);
    }

    {
        aeron_cluster_node_state_file_t *nsf = nullptr;
        ASSERT_EQ(0, aeron_cluster_node_state_file_open(&nsf, m_dir.c_str(), false, SYNC_LEVEL));
        ASSERT_NE(nullptr, nsf);

        EXPECT_EQ(next_candidate_term_id, aeron_cluster_node_state_file_candidate_term_id(nsf));

        aeron_cluster_node_state_file_close(nsf);
    }
}

TEST_F(NodeStateFileTest, shouldHandleNodeStateFileCreatedWithEarlierVersion)
{
    /*
     * Simulate a pre-existing node-state file created by an earlier version.
     * The C implementation uses a fixed binary layout so we create the file
     * directly with known values at the documented offsets.
     */
    {
        std::string path = m_dir + "/" + AERON_CLUSTER_NODE_STATE_FILE_FILENAME;
        FILE *fp = fopen(path.c_str(), "wb");
        ASSERT_NE(nullptr, fp);

        uint8_t buf[AERON_CLUSTER_NODE_STATE_FILE_LENGTH];
        memset(buf, 0, sizeof(buf));

        int32_t version = AERON_CLUSTER_NODE_STATE_FILE_VERSION;
        memcpy(buf + AERON_CLUSTER_NSF_VERSION_OFFSET, &version, sizeof(version));

        int64_t term_id = V_1_42_X_CANDIDATE_TERM_ID;
        memcpy(buf + AERON_CLUSTER_NSF_CANDIDATE_TERM_OFFSET, &term_id, sizeof(term_id));

        int64_t log_pos = V_1_42_X_LOG_POSITION;
        memcpy(buf + AERON_CLUSTER_NSF_LOG_POSITION_OFFSET, &log_pos, sizeof(log_pos));

        int64_t ts = V_1_42_X_TIMESTAMP_MS;
        memcpy(buf + AERON_CLUSTER_NSF_TIMESTAMP_OFFSET, &ts, sizeof(ts));

        ASSERT_EQ(sizeof(buf), fwrite(buf, 1, sizeof(buf), fp));
        fclose(fp);
    }

    const int64_t candidate_term_id= 10;
    const int64_t log_position= 20;
    const int64_t timestamp_ms= 30;
    const int64_t new_candidate_term_id = candidate_term_id + 100;

    {
        aeron_cluster_node_state_file_t *nsf = nullptr;
        ASSERT_EQ(0, aeron_cluster_node_state_file_open(&nsf, m_dir.c_str(), false, SYNC_LEVEL));
        ASSERT_NE(nullptr, nsf);

        EXPECT_EQ(V_1_42_X_CANDIDATE_TERM_ID, aeron_cluster_node_state_file_candidate_term_id(nsf));
        EXPECT_EQ(V_1_42_X_LOG_POSITION, aeron_cluster_node_state_file_log_position(nsf));
        EXPECT_EQ(V_1_42_X_TIMESTAMP_MS, aeron_cluster_node_state_file_timestamp(nsf));

        aeron_cluster_node_state_file_update_candidate_term_id(nsf, candidate_term_id, log_position, timestamp_ms);
        EXPECT_EQ(candidate_term_id, aeron_cluster_node_state_file_candidate_term_id(nsf));
        EXPECT_EQ(log_position, aeron_cluster_node_state_file_log_position(nsf));
        EXPECT_EQ(timestamp_ms, aeron_cluster_node_state_file_timestamp(nsf));

        const int64_t candidate_term_id_post_propose =
            aeron_cluster_node_state_file_propose_max_candidate_term_id(
                nsf, new_candidate_term_id, log_position, timestamp_ms);
        EXPECT_EQ(new_candidate_term_id, candidate_term_id_post_propose);
        EXPECT_EQ(new_candidate_term_id, aeron_cluster_node_state_file_candidate_term_id(nsf));

        aeron_cluster_node_state_file_close(nsf);
    }
}
