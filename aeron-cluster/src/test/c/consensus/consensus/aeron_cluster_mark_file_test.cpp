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
#include <string>
#include <sys/stat.h>
#include <unistd.h>

extern "C"
{
#include "aeron_cluster_mark_file.h"
}

/* -----------------------------------------------------------------------
 * Helpers for reading/writing raw fields in the mapped region
 * ----------------------------------------------------------------------- */

#define MEMBER_ID_OFFSET    64

static int32_t read_int32(const uint8_t *base, size_t offset)
{
    int32_t v;
    memcpy(&v, base + offset, sizeof(v));
    return v;
}

static int64_t read_int64(const uint8_t *base, size_t offset)
{
    int64_t v;
    memcpy(&v, base + offset, sizeof(v));
    return v;
}

static void write_int32(uint8_t *base, size_t offset, int32_t v)
{
    memcpy(base + offset, &v, sizeof(v));
}

/* -----------------------------------------------------------------------
 * Fixture: creates a temp directory and cleans it up
 * ----------------------------------------------------------------------- */

class ClusterMarkFileTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        char tmpl[] = "/tmp/cluster_mark_file_test_XXXXXX";
        ASSERT_NE(nullptr, mkdtemp(tmpl));
        m_temp_dir = tmpl;
        m_mark_file_path = m_temp_dir + "/" + AERON_CLUSTER_MARK_FILE_FILENAME;
    }

    void TearDown() override
    {
        /* Remove any file we created, then the directory */
        unlink(m_mark_file_path.c_str());
        rmdir(m_temp_dir.c_str());
    }

    std::string m_temp_dir;
    std::string m_mark_file_path;
};

/* =======================================================================
 * Test 1: shouldCreateNewMarkFile
 *
 * Java: shouldCreateNewMarkFile — open a new mark file, verify it exists
 * and has correct total size, then signal ready and check fields.
 * ======================================================================= */

TEST_F(ClusterMarkFileTest, shouldCreateNewMarkFile)
{
    aeron_cluster_mark_file_t *mf = nullptr;
    const int error_buf_len = AERON_CLUSTER_MARK_FILE_ERROR_BUFFER_MIN;
    const int64_t now_ms = 35984758934759843LL;

    ASSERT_EQ(0, aeron_cluster_mark_file_open(
        &mf, m_mark_file_path.c_str(),
        AERON_CLUSTER_COMPONENT_CONSENSUS_MODULE,
        error_buf_len, now_ms, 1000));
    ASSERT_NE(nullptr, mf);

    /* File should exist with correct total size */
    struct stat st;
    ASSERT_EQ(0, stat(m_mark_file_path.c_str(), &st));
    EXPECT_EQ(
        static_cast<off_t>(AERON_CLUSTER_MARK_FILE_HEADER_LENGTH + error_buf_len),
        st.st_size);

    /* Before signal_ready: version == 0 */
    EXPECT_EQ(0, read_int32(mf->mapped, AERON_CLUSTER_MARK_FILE_VERSION_OFFSET));

    /* Signal ready */
    aeron_cluster_mark_file_signal_ready(mf, now_ms);

    /* After signal_ready */
    EXPECT_EQ(AERON_CLUSTER_MARK_FILE_SEMANTIC_VERSION,
        read_int32(mf->mapped, AERON_CLUSTER_MARK_FILE_VERSION_OFFSET));
    EXPECT_EQ(now_ms,
        read_int64(mf->mapped, AERON_CLUSTER_MARK_FILE_ACTIVITY_TIMESTAMP_OFFSET));
    EXPECT_EQ(now_ms,
        read_int64(mf->mapped, AERON_CLUSTER_MARK_FILE_START_TIMESTAMP_OFFSET));
    EXPECT_EQ(static_cast<int32_t>(AERON_CLUSTER_COMPONENT_CONSENSUS_MODULE),
        read_int32(mf->mapped, AERON_CLUSTER_MARK_FILE_COMPONENT_TYPE_OFFSET));
    EXPECT_EQ(static_cast<int64_t>(getpid()),
        read_int64(mf->mapped, AERON_CLUSTER_MARK_FILE_PID_OFFSET));
    EXPECT_EQ(INT64_C(-1),
        read_int64(mf->mapped, AERON_CLUSTER_MARK_FILE_CANDIDATE_TERM_ID_OFFSET));

    aeron_cluster_mark_file_close(mf);
}

/* =======================================================================
 * Test 2: shouldUpdateActivityTimestamp
 *
 * Java: activityTimestampAccessors — update activity timestamp and read it.
 * ======================================================================= */

TEST_F(ClusterMarkFileTest, shouldUpdateActivityTimestamp)
{
    aeron_cluster_mark_file_t *mf = nullptr;
    ASSERT_EQ(0, aeron_cluster_mark_file_open(
        &mf, m_mark_file_path.c_str(),
        AERON_CLUSTER_COMPONENT_CONSENSUS_MODULE,
        AERON_CLUSTER_MARK_FILE_ERROR_BUFFER_MIN, 0, 1000));

    EXPECT_EQ(INT64_C(0),
        read_int64(mf->mapped, AERON_CLUSTER_MARK_FILE_ACTIVITY_TIMESTAMP_OFFSET));

    aeron_cluster_mark_file_update_activity_timestamp(mf, 4444555555LL);
    EXPECT_EQ(INT64_C(4444555555),
        read_int64(mf->mapped, AERON_CLUSTER_MARK_FILE_ACTIVITY_TIMESTAMP_OFFSET));

    aeron_cluster_mark_file_update_activity_timestamp(mf, 439856438756348LL);
    EXPECT_EQ(INT64_C(439856438756348),
        read_int64(mf->mapped, AERON_CLUSTER_MARK_FILE_ACTIVITY_TIMESTAMP_OFFSET));

    aeron_cluster_mark_file_close(mf);
}

/* =======================================================================
 * Test 3: shouldGetAndSetCandidateTermId
 *
 * Java: candidateTermIdAccessors — verify default is NULL_VALUE (-1),
 * then set and read back.
 * ======================================================================= */

TEST_F(ClusterMarkFileTest, shouldGetAndSetCandidateTermId)
{
    aeron_cluster_mark_file_t *mf = nullptr;
    ASSERT_EQ(0, aeron_cluster_mark_file_open(
        &mf, m_mark_file_path.c_str(),
        AERON_CLUSTER_COMPONENT_CONSENSUS_MODULE,
        AERON_CLUSTER_MARK_FILE_ERROR_BUFFER_MIN, 0, 1000));

    /* Default is -1 (NULL_VALUE) */
    EXPECT_EQ(INT64_C(-1), aeron_cluster_mark_file_candidate_term_id(mf));

    aeron_cluster_mark_file_set_candidate_term_id(mf, 123);
    EXPECT_EQ(INT64_C(123), aeron_cluster_mark_file_candidate_term_id(mf));

    aeron_cluster_mark_file_set_candidate_term_id(mf, 753475487LL);
    EXPECT_EQ(INT64_C(753475487), aeron_cluster_mark_file_candidate_term_id(mf));

    aeron_cluster_mark_file_close(mf);
}

/* =======================================================================
 * Test 4: shouldSignalReady
 *
 * Java: signalReady — sets version to SEMANTIC_VERSION and activity timestamp.
 * ======================================================================= */

TEST_F(ClusterMarkFileTest, shouldSignalReady)
{
    aeron_cluster_mark_file_t *mf = nullptr;
    ASSERT_EQ(0, aeron_cluster_mark_file_open(
        &mf, m_mark_file_path.c_str(),
        AERON_CLUSTER_COMPONENT_BACKUP,
        AERON_CLUSTER_MARK_FILE_ERROR_BUFFER_MIN, 0, 1000));

    /* Before signal_ready: version == 0 */
    EXPECT_EQ(0, read_int32(mf->mapped, AERON_CLUSTER_MARK_FILE_VERSION_OFFSET));

    aeron_cluster_mark_file_signal_ready(mf, 1000);
    EXPECT_EQ(AERON_CLUSTER_MARK_FILE_SEMANTIC_VERSION,
        read_int32(mf->mapped, AERON_CLUSTER_MARK_FILE_VERSION_OFFSET));
    EXPECT_EQ(INT64_C(1000),
        read_int64(mf->mapped, AERON_CLUSTER_MARK_FILE_ACTIVITY_TIMESTAMP_OFFSET));

    aeron_cluster_mark_file_close(mf);
}

/* =======================================================================
 * Test 5: shouldCheckIsActiveWhenReady
 *
 * Java: is_active — mark file is active when version > 0 and activity
 * timestamp is within timeout.
 * ======================================================================= */

TEST_F(ClusterMarkFileTest, shouldCheckIsActiveWhenReady)
{
    aeron_cluster_mark_file_t *mf = nullptr;
    const int64_t now_ms = 100000LL;
    ASSERT_EQ(0, aeron_cluster_mark_file_open(
        &mf, m_mark_file_path.c_str(),
        AERON_CLUSTER_COMPONENT_CONSENSUS_MODULE,
        AERON_CLUSTER_MARK_FILE_ERROR_BUFFER_MIN, now_ms, 1000));

    /* Not ready yet (version == 0) */
    EXPECT_FALSE(aeron_cluster_mark_file_is_active(
        m_mark_file_path.c_str(), now_ms, 50000));

    /* Signal ready */
    aeron_cluster_mark_file_signal_ready(mf, now_ms);

    /* Now active: version > 0 and timestamp within timeout */
    EXPECT_TRUE(aeron_cluster_mark_file_is_active(
        m_mark_file_path.c_str(), now_ms + 1000, 50000));

    aeron_cluster_mark_file_close(mf);
}

/* =======================================================================
 * Test 6: shouldNotBeActiveWhenTimedOut
 *
 * Java: is_active — mark file is NOT active when activity timestamp
 * is beyond the timeout window.
 * ======================================================================= */

TEST_F(ClusterMarkFileTest, shouldNotBeActiveWhenTimedOut)
{
    aeron_cluster_mark_file_t *mf = nullptr;
    const int64_t now_ms = 100000LL;
    ASSERT_EQ(0, aeron_cluster_mark_file_open(
        &mf, m_mark_file_path.c_str(),
        AERON_CLUSTER_COMPONENT_CONSENSUS_MODULE,
        AERON_CLUSTER_MARK_FILE_ERROR_BUFFER_MIN, now_ms, 1000));

    aeron_cluster_mark_file_signal_ready(mf, now_ms);

    /* Activity timestamp is now_ms, check at now_ms + timeout => timed out */
    EXPECT_FALSE(aeron_cluster_mark_file_is_active(
        m_mark_file_path.c_str(), now_ms + 50001, 50000));

    aeron_cluster_mark_file_close(mf);
}

/* =======================================================================
 * Test 7: shouldNotBeActiveForNonexistentFile
 *
 * Java: implicit — is_active returns false for non-existent file.
 * ======================================================================= */

TEST_F(ClusterMarkFileTest, shouldNotBeActiveForNonexistentFile)
{
    std::string bogus = m_temp_dir + "/no-such-file.dat";
    EXPECT_FALSE(aeron_cluster_mark_file_is_active(bogus.c_str(), 100000, 50000));
}

/* =======================================================================
 * Test 8: shouldWriteComponentType
 *
 * Java: shouldCreateNewMarkFile (per component type) — verify the
 * componentType field is stored correctly.
 * ======================================================================= */

struct ComponentTypeCase
{
    aeron_cluster_component_type_t type;
};

class MarkFileComponentTypeTest
    : public ClusterMarkFileTest,
      public ::testing::WithParamInterface<ComponentTypeCase>
{
};

static const ComponentTypeCase COMPONENT_TYPES[] = {
    {AERON_CLUSTER_COMPONENT_CONSENSUS_MODULE},
    {AERON_CLUSTER_COMPONENT_CONTAINER},
    {AERON_CLUSTER_COMPONENT_BACKUP},
};

INSTANTIATE_TEST_SUITE_P(
    ClusterMarkFile, MarkFileComponentTypeTest,
    ::testing::ValuesIn(COMPONENT_TYPES));

TEST_P(MarkFileComponentTypeTest, shouldWriteComponentType)
{
    auto ct = GetParam().type;
    aeron_cluster_mark_file_t *mf = nullptr;
    ASSERT_EQ(0, aeron_cluster_mark_file_open(
        &mf, m_mark_file_path.c_str(),
        ct, AERON_CLUSTER_MARK_FILE_ERROR_BUFFER_MIN, 0, 1000));

    EXPECT_EQ(static_cast<int32_t>(ct),
        read_int32(mf->mapped, AERON_CLUSTER_MARK_FILE_COMPONENT_TYPE_OFFSET));

    aeron_cluster_mark_file_close(mf);
}

/* =======================================================================
 * Test 9: shouldReadAndWriteClusterId
 *
 * Java: clusterIdAccessors — get/set clusterId via the mapped memory.
 * ======================================================================= */

TEST_F(ClusterMarkFileTest, shouldReadAndWriteClusterId)
{
    aeron_cluster_mark_file_t *mf = nullptr;
    ASSERT_EQ(0, aeron_cluster_mark_file_open(
        &mf, m_mark_file_path.c_str(),
        AERON_CLUSTER_COMPONENT_CONSENSUS_MODULE,
        AERON_CLUSTER_MARK_FILE_ERROR_BUFFER_MIN, 0, 1000));

    /* Default is 0 */
    EXPECT_EQ(0, read_int32(mf->mapped, AERON_CLUSTER_MARK_FILE_CLUSTER_ID_OFFSET));

    write_int32(mf->mapped, AERON_CLUSTER_MARK_FILE_CLUSTER_ID_OFFSET, 42);
    EXPECT_EQ(42, read_int32(mf->mapped, AERON_CLUSTER_MARK_FILE_CLUSTER_ID_OFFSET));

    write_int32(mf->mapped, AERON_CLUSTER_MARK_FILE_CLUSTER_ID_OFFSET, -9);
    EXPECT_EQ(-9, read_int32(mf->mapped, AERON_CLUSTER_MARK_FILE_CLUSTER_ID_OFFSET));

    aeron_cluster_mark_file_close(mf);
}

/* =======================================================================
 * Test 10: shouldReadAndWriteMemberId
 *
 * Java: memberIdAccessors — get/set memberId via the mapped memory.
 * ======================================================================= */

TEST_F(ClusterMarkFileTest, shouldReadAndWriteMemberId)
{
    aeron_cluster_mark_file_t *mf = nullptr;
    ASSERT_EQ(0, aeron_cluster_mark_file_open(
        &mf, m_mark_file_path.c_str(),
        AERON_CLUSTER_COMPONENT_CONSENSUS_MODULE,
        AERON_CLUSTER_MARK_FILE_ERROR_BUFFER_MIN, 0, 1000));

    /* Default is 0 */
    EXPECT_EQ(0, read_int32(mf->mapped, MEMBER_ID_OFFSET));

    write_int32(mf->mapped, MEMBER_ID_OFFSET, 7);
    EXPECT_EQ(7, read_int32(mf->mapped, MEMBER_ID_OFFSET));

    write_int32(mf->mapped, MEMBER_ID_OFFSET, -5);
    EXPECT_EQ(-5, read_int32(mf->mapped, MEMBER_ID_OFFSET));

    aeron_cluster_mark_file_close(mf);
}

/* =======================================================================
 * Test 11: shouldRejectActiveMarkFile
 *
 * Java: shouldUpdateExistingMarkFile — existing active file prevents
 * re-open until it goes stale.
 * ======================================================================= */

TEST_F(ClusterMarkFileTest, shouldRejectActiveMarkFile)
{
    aeron_cluster_mark_file_t *mf1 = nullptr;
    const int64_t now_ms = 100000LL;
    ASSERT_EQ(0, aeron_cluster_mark_file_open(
        &mf1, m_mark_file_path.c_str(),
        AERON_CLUSTER_COMPONENT_CONSENSUS_MODULE,
        AERON_CLUSTER_MARK_FILE_ERROR_BUFFER_MIN, now_ms, 1000));

    aeron_cluster_mark_file_signal_ready(mf1, now_ms);

    /* Try to open again while still active — should fail */
    aeron_cluster_mark_file_t *mf2 = nullptr;
    EXPECT_EQ(-1, aeron_cluster_mark_file_open(
        &mf2, m_mark_file_path.c_str(),
        AERON_CLUSTER_COMPONENT_CONSENSUS_MODULE,
        AERON_CLUSTER_MARK_FILE_ERROR_BUFFER_MIN, now_ms + 500, 1000));

    aeron_cluster_mark_file_close(mf1);
}

/* =======================================================================
 * Test 12: shouldSetAndGetAuthenticator
 *
 * Authenticator supplier name stored in the mark file header.
 * ======================================================================= */

TEST_F(ClusterMarkFileTest, shouldSetAndGetAuthenticator)
{
    aeron_cluster_mark_file_t *mf = nullptr;
    ASSERT_EQ(0, aeron_cluster_mark_file_open(
        &mf, m_mark_file_path.c_str(),
        AERON_CLUSTER_COMPONENT_CONSENSUS_MODULE,
        AERON_CLUSTER_MARK_FILE_ERROR_BUFFER_MIN, 0, 1000));

    const char *auth_class = "io.aeron.security.DefaultAuthenticatorSupplier";
    aeron_cluster_mark_file_set_authenticator(mf, auth_class);

    char buf[256];
    const char *result = aeron_cluster_mark_file_get_authenticator(mf, buf, sizeof(buf));
    EXPECT_STREQ(auth_class, result);

    aeron_cluster_mark_file_close(mf);
}

/* =======================================================================
 * Test 13: shouldGenerateFilenames
 *
 * Java: implicit — filename helpers produce correct names.
 * ======================================================================= */

TEST_F(ClusterMarkFileTest, shouldGenerateFilenames)
{
    char buf[256];

    aeron_cluster_mark_file_filename(buf, sizeof(buf));
    EXPECT_STREQ(AERON_CLUSTER_MARK_FILE_FILENAME, buf);

    aeron_cluster_mark_file_service_filename(buf, sizeof(buf), 0);
    EXPECT_STREQ("cluster-mark-service-0.dat", buf);

    aeron_cluster_mark_file_service_filename(buf, sizeof(buf), 3);
    EXPECT_STREQ("cluster-mark-service-3.dat", buf);
}

/* =======================================================================
 * Test 14 (bonus): shouldCloseNullSafely
 *
 * Java: shouldUnmapBufferUponClose — double close is safe.
 * ======================================================================= */

TEST_F(ClusterMarkFileTest, shouldCloseNullSafely)
{
    /* Close NULL should not crash */
    EXPECT_EQ(0, aeron_cluster_mark_file_close(nullptr));
}

/* =======================================================================
 * Test 15: shouldReopenStaleMarkFile
 *
 * Java: shouldUpdateExistingMarkFile — once a mark file goes stale,
 * re-opening should succeed and preserve the candidateTermId.
 * ======================================================================= */

TEST_F(ClusterMarkFileTest, shouldReopenStaleMarkFile)
{
    const int64_t now_ms = 100000LL;
    const int64_t candidate_term = 753475487LL;

    /* Create and signal ready */
    {
        aeron_cluster_mark_file_t *mf = nullptr;
        ASSERT_EQ(0, aeron_cluster_mark_file_open(
            &mf, m_mark_file_path.c_str(),
            AERON_CLUSTER_COMPONENT_CONSENSUS_MODULE,
            AERON_CLUSTER_MARK_FILE_ERROR_BUFFER_MIN, now_ms, 1000));

        aeron_cluster_mark_file_signal_ready(mf, now_ms);
        aeron_cluster_mark_file_set_candidate_term_id(mf, candidate_term);
        write_int32(mf->mapped, AERON_CLUSTER_MARK_FILE_CLUSTER_ID_OFFSET, -9);
        write_int32(mf->mapped, MEMBER_ID_OFFSET, 8);

        aeron_cluster_mark_file_close(mf);
    }

    /* Re-open after the file is stale (now_ms advanced beyond timeout) */
    {
        aeron_cluster_mark_file_t *mf = nullptr;
        const int64_t later_ms = now_ms + 100000;
        ASSERT_EQ(0, aeron_cluster_mark_file_open(
            &mf, m_mark_file_path.c_str(),
            AERON_CLUSTER_COMPONENT_CONSENSUS_MODULE,
            AERON_CLUSTER_MARK_FILE_ERROR_BUFFER_MIN, later_ms, 1000));

        /* Open re-creates/overwrites, so candidateTermId reverts to -1 */
        EXPECT_EQ(INT64_C(-1), aeron_cluster_mark_file_candidate_term_id(mf));

        /* Start timestamp should be later_ms */
        EXPECT_EQ(later_ms,
            read_int64(mf->mapped, AERON_CLUSTER_MARK_FILE_START_TIMESTAMP_OFFSET));

        aeron_cluster_mark_file_close(mf);
    }
}

/* =======================================================================
 * Test 16: shouldReturnParentDirectory
 * ======================================================================= */

TEST_F(ClusterMarkFileTest, shouldReturnParentDirectory)
{
    aeron_cluster_mark_file_t *mf = nullptr;
    ASSERT_EQ(0, aeron_cluster_mark_file_open(
        &mf, m_mark_file_path.c_str(),
        AERON_CLUSTER_COMPONENT_CONSENSUS_MODULE,
        AERON_CLUSTER_MARK_FILE_ERROR_BUFFER_MIN, 0, 1000));

    char buf[4096];
    const char *parent = aeron_cluster_mark_file_parent_directory(mf, buf, sizeof(buf));
    EXPECT_STREQ(m_temp_dir.c_str(), parent);

    aeron_cluster_mark_file_close(mf);
}

/* =======================================================================
 * Test 17: shouldNotBeActiveWhenVersionIsZero
 *
 * Java: version == 0 means not ready.
 * ======================================================================= */

TEST_F(ClusterMarkFileTest, shouldNotBeActiveWhenVersionIsZero)
{
    aeron_cluster_mark_file_t *mf = nullptr;
    ASSERT_EQ(0, aeron_cluster_mark_file_open(
        &mf, m_mark_file_path.c_str(),
        AERON_CLUSTER_COMPONENT_CONSENSUS_MODULE,
        AERON_CLUSTER_MARK_FILE_ERROR_BUFFER_MIN, 100000, 1000));

    /* Version is 0, so not active even with recent timestamp */
    aeron_cluster_mark_file_update_activity_timestamp(mf, 100000);
    EXPECT_FALSE(aeron_cluster_mark_file_is_active(
        m_mark_file_path.c_str(), 100000, 50000));

    aeron_cluster_mark_file_close(mf);
}

/* =======================================================================
 * Test 18: shouldStoreHeaderAndErrorBufferLengths
 *
 * Java: shouldCreateNewMarkFile — verify headerLength and
 * errorBufferLength stored in the file.
 * ======================================================================= */

TEST_F(ClusterMarkFileTest, shouldStoreHeaderAndErrorBufferLengths)
{
    aeron_cluster_mark_file_t *mf = nullptr;
    ASSERT_EQ(0, aeron_cluster_mark_file_open(
        &mf, m_mark_file_path.c_str(),
        AERON_CLUSTER_COMPONENT_CONSENSUS_MODULE,
        AERON_CLUSTER_MARK_FILE_ERROR_BUFFER_MIN, 0, 1000));

    EXPECT_EQ(AERON_CLUSTER_MARK_FILE_HEADER_LENGTH,
        read_int32(mf->mapped, AERON_CLUSTER_MARK_FILE_HEADER_LENGTH_OFFSET));
    EXPECT_EQ(AERON_CLUSTER_MARK_FILE_ERROR_BUFFER_MIN,
        read_int32(mf->mapped, AERON_CLUSTER_MARK_FILE_ERROR_BUFFER_LENGTH_OFFSET));

    aeron_cluster_mark_file_close(mf);
}
