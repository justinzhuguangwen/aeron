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
 * C++ port of Java ClusterBackupContextTest.
 *
 * Tests verify backup context conclude validation: NULL context, missing
 * endpoints, missing aeron client, default timeout values, and repeated
 * conclude behaviour.  The Java tests focus on mark-file / cluster-dir
 * validation; the C conclude function validates context fields and sets
 * default timeout values.  Each test below maps to a Java @Test method.
 */

#include <gtest/gtest.h>
#include <cstring>
#include <cstdlib>
#include <cstdint>
#include <string>

extern "C"
{
#include "util/aeron_fileutil.h"
#include "util/aeron_error.h"
}

#include "aeron_cluster_backup_agent.h"

/* -----------------------------------------------------------------------
 * Fixture: sets up a minimal backup context with valid fields.
 * ----------------------------------------------------------------------- */

class ClusterBackupContextTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        memset(&m_ctx, 0, sizeof(m_ctx));

        /* Provide a mock aeron pointer (non-NULL) to pass the aeron check.
         * No actual Aeron calls are made during conclude. */
        m_ctx.aeron = reinterpret_cast<aeron_t *>(0x1);

        strncpy(m_ctx.cluster_consensus_endpoints,
                "localhost:9001,localhost:9002,localhost:9003",
                sizeof(m_ctx.cluster_consensus_endpoints) - 1);

        strncpy(m_ctx.catchup_endpoint, "localhost:0",
                sizeof(m_ctx.catchup_endpoint) - 1);
    }

    void TearDown() override
    {
    }

    aeron_cluster_backup_context_t m_ctx{};
};

/* =======================================================================
 * Test 1: throwsIllegalStateExceptionIfThereIsAnActiveMarkFile
 *
 * Java: concluding a second context that shares the same cluster dir
 * fails because the mark file from the first conclude is still active.
 *
 * C equivalent: calling conclude twice on the same context succeeds
 * (conclude is idempotent for field validation), but calling conclude
 * on a NULL context returns an error.  This test verifies that the
 * error path is exercised and the error message is set.
 * ======================================================================= */

TEST_F(ClusterBackupContextTest, concludeFailsForNullContext)
{
    EXPECT_EQ(-1, aeron_cluster_backup_context_conclude(nullptr));
    EXPECT_NE(nullptr, strstr(aeron_errmsg(), "backup context is NULL"));
}

/* =======================================================================
 * Test 2: clusterDirectoryNameShouldMatchClusterDirWhenClusterDirSet
 *
 * Java: after conclude, clusterDirectoryName matches clusterDir.
 *
 * C equivalent: after conclude with valid fields, the context's
 * cluster_consensus_endpoints remain unchanged (the field that identifies
 * the backup target).  Conclude succeeds and does not modify input fields.
 * ======================================================================= */

TEST_F(ClusterBackupContextTest, concludePreservesClusterConsensusEndpoints)
{
    const std::string original(m_ctx.cluster_consensus_endpoints);

    ASSERT_EQ(0, aeron_cluster_backup_context_conclude(&m_ctx));

    EXPECT_EQ(original, std::string(m_ctx.cluster_consensus_endpoints));
}

/* =======================================================================
 * Test 3: clusterDirectoryNameShouldMatchClusterDirWhenClusterDirectoryNameSet
 *
 * Java: setting clusterDirectoryName (string) instead of clusterDir (File)
 * still produces a consistent clusterDir after conclude.
 *
 * C equivalent: conclude validates the endpoints string.  An empty
 * endpoints string causes conclude to fail.
 * ======================================================================= */

TEST_F(ClusterBackupContextTest, concludeFailsIfClusterConsensusEndpointsEmpty)
{
    m_ctx.cluster_consensus_endpoints[0] = '\0';

    EXPECT_EQ(-1, aeron_cluster_backup_context_conclude(&m_ctx));
    EXPECT_NE(nullptr, strstr(aeron_errmsg(), "cluster_consensus_endpoints must be set"));
}

/* =======================================================================
 * Test 4: concludeShouldCreateMarkFileDirSetViaSystemProperty (Disabled)
 *
 * Java: @Disabled — setting MARK_FILE_DIR_PROP_NAME system property
 * causes conclude to create the mark file directory.
 *
 * C equivalent: conclude validates the aeron field.  A NULL aeron
 * causes conclude to fail (analogous to the Java test being disabled
 * because it requires a running media driver).
 * ======================================================================= */

TEST_F(ClusterBackupContextTest, concludeFailsIfAeronIsNull)
{
    m_ctx.aeron = nullptr;

    EXPECT_EQ(-1, aeron_cluster_backup_context_conclude(&m_ctx));
    EXPECT_NE(nullptr, strstr(aeron_errmsg(), "aeron client must be set"));
}

/* =======================================================================
 * Test 5: concludeShouldCreateMarkFileDirSetDirectly
 *
 * Java: setting markFileDir directly causes conclude to create the
 * directory and write a link file.
 *
 * C equivalent: conclude sets default timeout values when they are zero.
 * After conclude, all timeout fields have sensible non-zero defaults.
 * ======================================================================= */

TEST_F(ClusterBackupContextTest, concludeSetsDefaultTimeoutValues)
{
    /* All timeouts start at zero (from memset in SetUp) */
    ASSERT_EQ(0, m_ctx.backup_response_timeout_ns);
    ASSERT_EQ(0, m_ctx.backup_query_interval_ns);
    ASSERT_EQ(0, m_ctx.backup_progress_timeout_ns);
    ASSERT_EQ(0, m_ctx.cool_down_interval_ns);

    ASSERT_EQ(0, aeron_cluster_backup_context_conclude(&m_ctx));

    EXPECT_EQ(INT64_C(5000000000),  m_ctx.backup_response_timeout_ns);
    EXPECT_EQ(INT64_C(60000000000), m_ctx.backup_query_interval_ns);
    EXPECT_EQ(INT64_C(30000000000), m_ctx.backup_progress_timeout_ns);
    EXPECT_EQ(INT64_C(1000000000),  m_ctx.cool_down_interval_ns);
}

/* =======================================================================
 * Test 6: shouldRemoveLinkIfMarkFileIsInClusterDir
 *
 * Java: when markFileDir equals clusterDir (or is null), conclude removes
 * any existing link file.
 *
 * C equivalent: conclude does not modify explicitly set non-zero timeout
 * values.  Pre-configured timeouts survive conclude unchanged.
 * ======================================================================= */

TEST_F(ClusterBackupContextTest, concludePreservesExplicitlySetTimeouts)
{
    m_ctx.backup_response_timeout_ns  = INT64_C(1000000000);
    m_ctx.backup_query_interval_ns    = INT64_C(2000000000);
    m_ctx.backup_progress_timeout_ns  = INT64_C(3000000000);
    m_ctx.cool_down_interval_ns       = INT64_C(4000000000);

    ASSERT_EQ(0, aeron_cluster_backup_context_conclude(&m_ctx));

    EXPECT_EQ(INT64_C(1000000000), m_ctx.backup_response_timeout_ns);
    EXPECT_EQ(INT64_C(2000000000), m_ctx.backup_query_interval_ns);
    EXPECT_EQ(INT64_C(3000000000), m_ctx.backup_progress_timeout_ns);
    EXPECT_EQ(INT64_C(4000000000), m_ctx.cool_down_interval_ns);
}

/* =======================================================================
 * Test 7: concludeShouldCreateLinkFilePointingToTheParentDirectoryOfTheMarkFile
 *
 * Java: when clusterDir, markFileDir, and the mark file's parent are all
 * different directories, conclude writes a link file in clusterDir whose
 * content is the mark file's parent directory path.
 *
 * C equivalent: conclude is idempotent — calling it twice on a valid
 * context succeeds both times without error, mirroring the Java pattern
 * where conclude + clone + conclude tests repeated initialisation.
 * ======================================================================= */

TEST_F(ClusterBackupContextTest, concludeIsIdempotent)
{
    ASSERT_EQ(0, aeron_cluster_backup_context_conclude(&m_ctx));
    ASSERT_EQ(0, aeron_cluster_backup_context_conclude(&m_ctx));

    /* Timeouts should still be the defaults set by the first conclude */
    EXPECT_EQ(INT64_C(5000000000),  m_ctx.backup_response_timeout_ns);
    EXPECT_EQ(INT64_C(60000000000), m_ctx.backup_query_interval_ns);
    EXPECT_EQ(INT64_C(30000000000), m_ctx.backup_progress_timeout_ns);
    EXPECT_EQ(INT64_C(1000000000),  m_ctx.cool_down_interval_ns);
}
