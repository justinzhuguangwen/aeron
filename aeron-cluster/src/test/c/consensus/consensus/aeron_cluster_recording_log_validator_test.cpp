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
 * C port of Java RecordingLogValidatorTest.java (1 test case).
 *
 * Java test uses Mockito to mock AeronArchive.getStartPosition() so that
 * certain recording IDs throw ArchiveException(UNKNOWN_RECORDING).
 * The validator then iterates snapshot entries and invalidates those whose
 * recordings are unknown.
 *
 * In C there is no production RecordingLogValidator yet, so we implement
 * the validation logic inline using the recording log API:
 *   1. Collect recording IDs from valid SNAPSHOT entries.
 *   2. Mark specified IDs as "unknown" (simulating the archive response).
 *   3. Invalidate entries whose recording_id is in the unknown set.
 *   4. Verify entry validity.
 */

#include <gtest/gtest.h>
#include <cstring>
#include <cstdlib>
#include <cstdint>
#include <string>
#include <set>

extern "C"
{
#include "aeron_common.h"
#include "util/aeron_fileutil.h"
#include "aeron_cluster_recording_log.h"
}

static std::string make_test_dir(const char *prefix)
{
    char base[AERON_MAX_PATH] = {0};
    aeron_temp_filename(base, sizeof(base));
    return std::string(base) + "-" + prefix;
}

/* -----------------------------------------------------------------------
 * Constants
 * ----------------------------------------------------------------------- */
static constexpr int32_t CM_SERVICE_ID = -1;
static constexpr int32_t SERVICE_ID_0= 0;

class RecordingLogValidatorTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        m_dir = make_test_dir("recording_log_validator_test_");
        aeron_delete_directory(m_dir.c_str());
        aeron_mkdir_recursive(m_dir.c_str(), 0777);
    }

    void TearDown() override
    {
        aeron_delete_directory(m_dir.c_str());
    }

    std::string m_dir;
};

/* -----------------------------------------------------------------------
 * shouldInvalidateSnapshotsWithUnknownRecordings
 *
 * Java test layout:
 *   recordingLog.appendSnapshot(0, 0, 0, 100, 0, 0);       // svc 0, pos 100
 *   recordingLog.appendSnapshot(1, 0, 0, 100, 0, -1);      // CM,   pos 100
 *   recordingLog.appendSnapshot(2, 0, 0, 200, 0, 0);       // svc 0, pos 200
 *   recordingLog.appendSnapshot(3, 0, 0, 200, 0, -1);      // CM,   pos 200
 *   recordingLog.appendTerm(4, 1, 0, 0);                    // TERM
 *   recordingLog.appendStandbySnapshot(0, 1, 0, 300, 0, 0, "localhost:1234");
 *   recordingLog.appendStandbySnapshot(1, 1, 0, 300, 0, 0, "localhost:1234");
 *
 * Recording IDs 0 and 1 are "unknown" (archive returns UNKNOWN_RECORDING).
 * After validation:
 *   - Snapshot entries with recording_id 0 or 1 should be invalidated.
 *   - All other entries (snapshots 2,3; term 4; standbys) stay valid.
 *   - Total entry count unchanged.
 * ----------------------------------------------------------------------- */
TEST_F(RecordingLogValidatorTest, shouldInvalidateSnapshotsWithUnknownRecordings)
{
    aeron_cluster_recording_log_t *log = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));

    /* Append entries matching Java test */
    ASSERT_EQ(0, aeron_cluster_recording_log_append_snapshot(
        log, 0, 0, 0, 100, 0, SERVICE_ID_0));   /* recording_id=0, svc=0, pos=100 */
    ASSERT_EQ(0, aeron_cluster_recording_log_append_snapshot(
        log, 1, 0, 0, 100, 0, CM_SERVICE_ID));  /* recording_id=1, svc=-1, pos=100 */
    ASSERT_EQ(0, aeron_cluster_recording_log_append_snapshot(
        log, 2, 0, 0, 200, 0, SERVICE_ID_0));   /* recording_id=2, svc=0, pos=200 */
    ASSERT_EQ(0, aeron_cluster_recording_log_append_snapshot(
        log, 3, 0, 0, 200, 0, CM_SERVICE_ID));  /* recording_id=3, svc=-1, pos=200 */
    ASSERT_EQ(0, aeron_cluster_recording_log_append_term(
        log, 4, 1, 0, 0));                      /* TERM entry */
    ASSERT_EQ(0, aeron_cluster_recording_log_append_standby_snapshot(
        log, 0, 1, 0, 300, 0, SERVICE_ID_0, "localhost:1234"));
    ASSERT_EQ(0, aeron_cluster_recording_log_append_standby_snapshot(
        log, 1, 1, 0, 300, 0, SERVICE_ID_0, "localhost:1234"));

    const int initial_count = log->sorted_count;

    /* Define the "unknown" recording IDs — simulates archive returning
     * UNKNOWN_RECORDING for getStartPosition(0) and getStartPosition(1) */
    std::set<int64_t> unknown_recording_ids;
    unknown_recording_ids.insert(0);
    unknown_recording_ids.insert(1);

    /*
     * Implement the validator logic (mirrors Java RecordingLogValidator.poll()):
     *
     * Phase 1: Collect recording IDs from valid SNAPSHOT entries.
     * Phase 2: Check each against the "archive" (our unknown set).
     * Phase 3: Invalidate SNAPSHOT entries with unknown recording IDs.
     */

    /* Phase 1+2: identify which snapshot recording IDs are unknown */
    std::set<int64_t> found_unknown;
    for (int i = 0; i < log->sorted_count; i++)
    {
        aeron_cluster_recording_log_entry_t *e = aeron_cluster_recording_log_entry_at(log, i);
        ASSERT_NE(nullptr, e);
        int base_type = e->entry_type & ~AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_INVALID_FLAG;
        if (base_type == AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_SNAPSHOT && e->is_valid)
        {
            if (unknown_recording_ids.count(e->recording_id) > 0)
            {
                found_unknown.insert(e->recording_id);
            }
        }
    }

    /* Phase 3: invalidate SNAPSHOT entries with unknown recording IDs.
     * Note: aeron_cluster_recording_log_invalidate_entry takes a sorted_index
     * and re-sorts after each invalidation, so we must re-scan from the start
     * after each invalidation to avoid stale indices. */
    bool invalidated = true;
    while (invalidated)
    {
        invalidated = false;
        for (int i = 0; i < log->sorted_count; i++)
        {
            aeron_cluster_recording_log_entry_t *e = aeron_cluster_recording_log_entry_at(log, i);
            ASSERT_NE(nullptr, e);
            int base_type = e->entry_type & ~AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_INVALID_FLAG;
            if (base_type == AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_SNAPSHOT &&
                e->is_valid &&
                found_unknown.count(e->recording_id) > 0)
            {
                ASSERT_EQ(0, aeron_cluster_recording_log_invalidate_entry(log, i));
                invalidated = true;
                break;  /* re-scan from start since sorted view changed */
            }
        }
    }

    /* Verify: entries with recording_id 0 or 1 that are SNAPSHOT type should be invalid.
     * All other entries should remain valid.
     * Total entry count should be unchanged. */
    EXPECT_EQ(initial_count, log->sorted_count);

    for (int i = 0; i < log->sorted_count; i++)
    {
        aeron_cluster_recording_log_entry_t *e = aeron_cluster_recording_log_entry_at(log, i);
        ASSERT_NE(nullptr, e);
        int base_type = e->entry_type & ~AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_INVALID_FLAG;

        if (base_type == AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_SNAPSHOT &&
            (e->recording_id == 0 || e->recording_id == 1))
        {
            EXPECT_FALSE(e->is_valid)
                << "Snapshot entry with recording_id=" << e->recording_id
                << " should be invalidated";
        }
        else
        {
            EXPECT_TRUE(e->is_valid)
                << "Entry with recording_id=" << e->recording_id
                << " entry_type=" << e->entry_type
                << " should remain valid";
        }
    }

    aeron_cluster_recording_log_close(log);
}
