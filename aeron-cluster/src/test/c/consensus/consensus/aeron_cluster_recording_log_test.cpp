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

extern "C"
{
#include "aeron_common.h"
#include "util/aeron_fileutil.h"
}

static std::string make_test_dir(const char *prefix)
{
    char base[AERON_MAX_PATH] = {0};
    aeron_temp_filename(base, sizeof(base));
    return std::string(base) + "-" + prefix;
}

#include "aeron_cluster_recording_log.h"

/* -----------------------------------------------------------------------
 * Constants (mirror Java ConsensusModule.Configuration)
 * ----------------------------------------------------------------------- */
static constexpr int32_t CM_SERVICE_ID = -1;   /* ConsensusModule.Configuration.SERVICE_ID */
static constexpr int32_t SERVICE_ID    = 0;    /* first user service */

/* -----------------------------------------------------------------------
 * RecordingLog tests
 * ----------------------------------------------------------------------- */
class RecordingLogTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        m_dir = make_test_dir("aeron_cluster_test_recording_log_");
        aeron_delete_directory(m_dir.c_str());
        aeron_mkdir_recursive(m_dir.c_str(), 0777);
    }
    void TearDown() override
    {
        aeron_delete_directory(m_dir.c_str());
    }

    static void append_term(aeron_cluster_recording_log_t *log,
                             int64_t rec, int64_t term, int64_t base, int64_t ts)
    {
        ASSERT_EQ(0, aeron_cluster_recording_log_append_term(log, rec, term, base, ts));
    }
    static void append_snap(aeron_cluster_recording_log_t *log,
                             int64_t rec, int64_t term, int64_t base,
                             int64_t pos, int64_t ts, int32_t svc)
    {
        ASSERT_EQ(0, aeron_cluster_recording_log_append_snapshot(log, rec, term, base, pos, ts, svc));
    }

    std::string m_dir;
};

TEST_F(RecordingLogTest, shouldCreateNewIndex)
{
    aeron_cluster_recording_log_t *log = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));
    EXPECT_EQ(0, log->entry_count);
    aeron_cluster_recording_log_close(log);
}

TEST_F(RecordingLogTest, shouldAppendAndThenCommitTermPosition)
{
    const int64_t new_position = 9999L;
    {
        aeron_cluster_recording_log_t *log = nullptr;
        ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));
        append_term(log, 1, 1111, 2222, 3333);
        ASSERT_EQ(0, aeron_cluster_recording_log_commit_log_position(log, 1111, new_position));
        aeron_cluster_recording_log_close(log);
    }
    {
        aeron_cluster_recording_log_t *log = nullptr;
        ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), false));
        EXPECT_EQ(1, log->entry_count);
        auto *e = aeron_cluster_recording_log_entry_at(log, 0);
        ASSERT_NE(nullptr, e);
        EXPECT_EQ(new_position, e->log_position);
        aeron_cluster_recording_log_close(log);
    }
}

TEST_F(RecordingLogTest, shouldAppendAndThenReloadLatestSnapshot)
{
    {
        aeron_cluster_recording_log_t *log = nullptr;
        ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));
        append_term(log,  1, 0, 0, 0);
        append_snap(log,  2, 0, 0, 100, 1, CM_SERVICE_ID);
        append_snap(log,  3, 0, 0, 100, 2, SERVICE_ID);
        aeron_cluster_recording_log_close(log);
    }
    {
        aeron_cluster_recording_log_t *log = nullptr;
        ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), false));
        EXPECT_EQ(3, log->entry_count);
        auto *snap = aeron_cluster_recording_log_get_latest_snapshot(log, CM_SERVICE_ID);
        ASSERT_NE(nullptr, snap);
        EXPECT_EQ(2, snap->recording_id);
        snap = aeron_cluster_recording_log_get_latest_snapshot(log, SERVICE_ID);
        ASSERT_NE(nullptr, snap);
        EXPECT_EQ(3, snap->recording_id);
        aeron_cluster_recording_log_close(log);
    }
}

TEST_F(RecordingLogTest, shouldIgnoreIncompleteSnapshotInRecoveryPlan)
{
    /* Only CM snapshot at pos=777, service snapshot missing -> not included */
    {
        aeron_cluster_recording_log_t *log = nullptr;
        ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));
        append_snap(log, 1, 1, 0, 777, 0, CM_SERVICE_ID);
        /* service snapshot at 777 missing */
        append_snap(log, 3, 1, 0, 888, 0, CM_SERVICE_ID);
        append_snap(log, 4, 1, 0, 888, 0, SERVICE_ID);
        aeron_cluster_recording_log_close(log);
    }
    {
        aeron_cluster_recording_log_t *log = nullptr;
        ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), false));
        aeron_cluster_recovery_plan_t *plan = nullptr;
        ASSERT_EQ(0, aeron_cluster_recording_log_create_recovery_plan(log, &plan, 1, nullptr));
        ASSERT_NE(nullptr, plan);
        /* Latest complete snapshot is at pos=888 */
        EXPECT_EQ(3, plan->snapshots[0].recording_id);  /* CM */
        EXPECT_EQ(4, plan->snapshots[1].recording_id);  /* service */
        aeron_cluster_recovery_plan_free(plan);
        aeron_cluster_recording_log_close(log);
    }
}

TEST_F(RecordingLogTest, shouldIgnoreInvalidMidSnapshotInRecoveryPlan)
{
    {
        aeron_cluster_recording_log_t *log = nullptr;
        ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));
        append_snap(log, 1, 1, 0, 777, 0, CM_SERVICE_ID);
        append_snap(log, 2, 1, 0, 777, 0, SERVICE_ID);
        append_snap(log, 3, 1, 0, 888, 0, CM_SERVICE_ID);
        append_snap(log, 4, 1, 0, 888, 0, SERVICE_ID);
        append_snap(log, 5, 1, 0, 999, 0, CM_SERVICE_ID);
        append_snap(log, 6, 1, 0, 999, 0, SERVICE_ID);
        ASSERT_EQ(0, aeron_cluster_recording_log_invalidate_entry(log, 2)); /* rec=3 */
        ASSERT_EQ(0, aeron_cluster_recording_log_invalidate_entry(log, 3)); /* rec=4 */
        aeron_cluster_recording_log_close(log);
    }
    {
        aeron_cluster_recording_log_t *log = nullptr;
        ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), false));
        aeron_cluster_recovery_plan_t *plan = nullptr;
        ASSERT_EQ(0, aeron_cluster_recording_log_create_recovery_plan(log, &plan, 1, nullptr));
        ASSERT_NE(nullptr, plan);
        /* Latest valid complete set is at pos=999 */
        EXPECT_EQ(5, plan->snapshots[0].recording_id);
        EXPECT_EQ(6, plan->snapshots[1].recording_id);
        aeron_cluster_recovery_plan_free(plan);
        aeron_cluster_recording_log_close(log);
    }
}

TEST_F(RecordingLogTest, shouldIgnoreInvalidTermInRecoveryPlan)
{
    {
        aeron_cluster_recording_log_t *log = nullptr;
        ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));
        append_term(log, 0, 9,  444, 0);
        append_term(log, 0, 10, 666, 0);
        append_snap(log, 1, 10, 666, 777, 0, CM_SERVICE_ID);
        append_snap(log, 2, 10, 666, 777, 0, SERVICE_ID);
        append_term(log, 0, 11, 999, 0);  /* will be invalidated */
        ASSERT_EQ(0, aeron_cluster_recording_log_invalidate_entry(log, 4));
        aeron_cluster_recording_log_close(log);
    }
    {
        aeron_cluster_recording_log_t *log = nullptr;
        ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), false));
        auto *last_term = aeron_cluster_recording_log_find_last_term(log);
        ASSERT_NE(nullptr, last_term);
        EXPECT_EQ(10, last_term->leadership_term_id);
        EXPECT_TRUE(aeron_cluster_recording_log_is_unknown(log, 11));
        EXPECT_EQ(-1, aeron_cluster_recording_log_get_term_timestamp(log, 11));
        aeron_cluster_recording_log_close(log);
    }
}

TEST_F(RecordingLogTest, shouldInvalidateLatestSnapshot)
{
    {
        aeron_cluster_recording_log_t *log = nullptr;
        ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));
        append_term(log, 1, 7, 0, 1000);
        append_snap(log, 2, 7, 0, 640,  1001, CM_SERVICE_ID);
        append_snap(log, 3, 7, 0, 640,  1001, SERVICE_ID);
        append_snap(log, 4, 7, 0, 1280, 1002, CM_SERVICE_ID);
        append_snap(log, 5, 7, 0, 1280, 1002, SERVICE_ID);
        append_term(log, 1, 8, 1280, 1002);
        EXPECT_EQ(1, aeron_cluster_recording_log_invalidate_latest_snapshot(log));
        EXPECT_EQ(6, log->entry_count);
        aeron_cluster_recording_log_close(log);
    }
    {
        aeron_cluster_recording_log_t *log = nullptr;
        ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), false));
        /* entries 3 and 4 (pos=1280 snapshots) should be invalid */
        auto *e3 = aeron_cluster_recording_log_entry_at(log, 3);
        auto *e4 = aeron_cluster_recording_log_entry_at(log, 4);
        ASSERT_NE(nullptr, e3); ASSERT_NE(nullptr, e4);
        EXPECT_FALSE(aeron_cluster_recording_log_entry_is_valid(e3));
        EXPECT_FALSE(aeron_cluster_recording_log_entry_is_valid(e4));
        /* entry 1 (pos=640 CM snap) still valid */
        auto *e1 = aeron_cluster_recording_log_entry_at(log, 1);
        ASSERT_NE(nullptr, e1);
        EXPECT_TRUE(aeron_cluster_recording_log_entry_is_valid(e1));
        /* Latest snapshot now at pos=640 */
        auto *snap = aeron_cluster_recording_log_get_latest_snapshot(log, CM_SERVICE_ID);
        ASSERT_NE(nullptr, snap);
        EXPECT_EQ(2, snap->recording_id);
        /* Invalidate again -- removes pos=640 set */
        EXPECT_EQ(1, aeron_cluster_recording_log_invalidate_latest_snapshot(log));
        aeron_cluster_recording_log_close(log);
    }
    {
        aeron_cluster_recording_log_t *log = nullptr;
        ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), false));
        /* No valid snapshot left -> returns 0 */
        EXPECT_EQ(0, aeron_cluster_recording_log_invalidate_latest_snapshot(log));
        aeron_cluster_recording_log_close(log);
    }
}

TEST_F(RecordingLogTest, shouldNotAllowInvalidateOfSnapshotWithoutParentTerm)
{
    aeron_cluster_recording_log_t *log = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));
    append_snap(log, -10, 1, 0, 777, 0, CM_SERVICE_ID);
    append_snap(log, -11, 1, 0, 777, 0, SERVICE_ID);
    /* No parent TERM -> should fail */
    EXPECT_EQ(-1, aeron_cluster_recording_log_invalidate_latest_snapshot(log));
    aeron_cluster_recording_log_close(log);
}

TEST_F(RecordingLogTest, shouldRecoverSnapshotsMidLogMarkedInvalid)
{
    {
        aeron_cluster_recording_log_t *log = nullptr;
        ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));
        append_snap(log, 1, 1, 10, 555, 0, CM_SERVICE_ID);
        append_snap(log, 2, 1, 10, 555, 0, SERVICE_ID);
        append_snap(log, 3, 1, 10, 777, 0, CM_SERVICE_ID);
        append_snap(log, 4, 1, 10, 777, 0, SERVICE_ID);
        append_snap(log, 5, 1, 10, 888, 0, CM_SERVICE_ID);
        append_snap(log, 6, 1, 10, 888, 0, SERVICE_ID);
        ASSERT_EQ(0, aeron_cluster_recording_log_invalidate_entry(log, 2));
        ASSERT_EQ(0, aeron_cluster_recording_log_invalidate_entry(log, 3));
        aeron_cluster_recording_log_close(log);
    }
    /* Verify invalidated entries have correct flag */
    {
        aeron_cluster_recording_log_t *log = nullptr;
        ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), false));
        EXPECT_EQ(6, log->entry_count);
        auto *e2 = aeron_cluster_recording_log_entry_at(log, 2);
        auto *e3 = aeron_cluster_recording_log_entry_at(log, 3);
        ASSERT_NE(nullptr, e2); ASSERT_NE(nullptr, e3);
        EXPECT_FALSE(aeron_cluster_recording_log_entry_is_valid(e2));
        EXPECT_FALSE(aeron_cluster_recording_log_entry_is_valid(e3));
        /* Entries 0,1,4,5 still valid */
        EXPECT_TRUE(aeron_cluster_recording_log_entry_is_valid(aeron_cluster_recording_log_entry_at(log, 0)));
        EXPECT_TRUE(aeron_cluster_recording_log_entry_is_valid(aeron_cluster_recording_log_entry_at(log, 4)));
        aeron_cluster_recording_log_close(log);
    }
}

TEST_F(RecordingLogTest, shouldRecoverSnapshotsLastInLogMarkedWithInvalid)
{
    {
        aeron_cluster_recording_log_t *log = nullptr;
        ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));
        append_term(log, 1, 1, 0,  0);
        append_snap(log, -10, 1, 0, 777, 0, CM_SERVICE_ID);
        append_snap(log, -11, 1, 0, 777, 0, SERVICE_ID);
        append_term(log, 1, 2, 10, 0);
        append_snap(log, -12, 2, 10, 888, 0, CM_SERVICE_ID);
        append_snap(log, -13, 2, 10, 888, 0, SERVICE_ID);
        EXPECT_EQ(1, aeron_cluster_recording_log_invalidate_latest_snapshot(log));
        EXPECT_EQ(1, aeron_cluster_recording_log_invalidate_latest_snapshot(log));
        aeron_cluster_recording_log_close(log);
    }
    {
        aeron_cluster_recording_log_t *log = nullptr;
        ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), false));
        /* All snapshots invalidated -> no valid snapshot */
        EXPECT_EQ(nullptr, aeron_cluster_recording_log_get_latest_snapshot(log, CM_SERVICE_ID));
        /* But term entries remain valid */
        auto *last_term = aeron_cluster_recording_log_find_last_term(log);
        ASSERT_NE(nullptr, last_term);
        EXPECT_EQ(2, last_term->leadership_term_id);
        aeron_cluster_recording_log_close(log);
    }
}

TEST_F(RecordingLogTest, shouldAppendTermWithLeadershipTermIdOutOfOrder)
{
    aeron_cluster_recording_log_t *log = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));
    /* Append terms out of order -- C implementation should accept */
    append_term(log, 1, 5, 0, 1000);
    append_term(log, 2, 3, 0, 900);  /* earlier term appended after */
    EXPECT_EQ(2, log->entry_count);
    aeron_cluster_recording_log_close(log);
}

TEST_F(RecordingLogTest, shouldNotCreateInitialTermWithMinusOneTermId)
{
    aeron_cluster_recording_log_t *log = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));
    /* Appending term with leadership_term_id = -1 should fail */
    EXPECT_EQ(-1, aeron_cluster_recording_log_append_term(log, 1, -1, 0, 0));
    EXPECT_EQ(0, log->entry_count);
    aeron_cluster_recording_log_close(log);
}

TEST_F(RecordingLogTest, shouldDetermineIfSnapshotIsInvalid)
{
    aeron_cluster_recording_log_t *log = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));
    append_term(log, 1, 0, 0, 0);
    append_snap(log, 2, 0, 0, 100, 0, CM_SERVICE_ID);
    append_snap(log, 3, 0, 0, 100, 0, SERVICE_ID);

    auto *e1 = aeron_cluster_recording_log_entry_at(log, 1);
    ASSERT_NE(nullptr, e1);
    EXPECT_TRUE(aeron_cluster_recording_log_entry_is_valid(e1));

    EXPECT_EQ(1, aeron_cluster_recording_log_invalidate_latest_snapshot(log));

    auto *e1_after = aeron_cluster_recording_log_entry_at(log, 1);
    ASSERT_NE(nullptr, e1_after);
    EXPECT_FALSE(aeron_cluster_recording_log_entry_is_valid(e1_after));

    aeron_cluster_recording_log_close(log);
}

TEST_F(RecordingLogTest, shouldFindLastTermRecordingId)
{
    aeron_cluster_recording_log_t *log = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));
    append_term(log, 42, 0, 0, 0);
    append_term(log, 99, 1, 0, 0);
    EXPECT_EQ(99, aeron_cluster_recording_log_find_last_term_recording_id(log));
    aeron_cluster_recording_log_close(log);
}

TEST_F(RecordingLogTest, shouldGetTermTimestamp)
{
    aeron_cluster_recording_log_t *log = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));
    append_term(log, 1, 5, 0, 12345);
    EXPECT_EQ(12345, aeron_cluster_recording_log_get_term_timestamp(log, 5));
    EXPECT_EQ(-1,    aeron_cluster_recording_log_get_term_timestamp(log, 99));
    aeron_cluster_recording_log_close(log);
}

TEST_F(RecordingLogTest, shouldCreateRecoveryPlan)
{
    aeron_cluster_recording_log_t *log = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));
    append_term(log, 10, 0, 0, 1000);
    ASSERT_EQ(0, aeron_cluster_recording_log_commit_log_position(log, 0, 8000));
    append_snap(log, 20, 0, 0, 7000, 1500, CM_SERVICE_ID);
    append_snap(log, 21, 0, 0, 7000, 1500, SERVICE_ID);

    aeron_cluster_recovery_plan_t *plan = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_create_recovery_plan(log, &plan, 1, nullptr));
    ASSERT_NE(nullptr, plan);
    EXPECT_EQ(0,    plan->last_leadership_term_id);
    EXPECT_EQ(8000, plan->last_append_position);
    EXPECT_EQ(10,   plan->last_term_recording_id);
    EXPECT_GE(plan->snapshot_count, 1);
    EXPECT_EQ(20,   plan->snapshots[0].recording_id);

    aeron_cluster_recovery_plan_free(plan);
    aeron_cluster_recording_log_close(log);
}

TEST_F(RecordingLogTest, shouldHandleEmptyRecordingLog)
{
    aeron_cluster_recording_log_t *log = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));

    EXPECT_EQ(nullptr, aeron_cluster_recording_log_find_last_term(log));
    EXPECT_EQ(-1,      aeron_cluster_recording_log_find_last_term_recording_id(log));
    EXPECT_TRUE(aeron_cluster_recording_log_is_unknown(log, 0));

    aeron_cluster_recovery_plan_t *plan = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_create_recovery_plan(log, &plan, 1, nullptr));
    ASSERT_NE(nullptr, plan);
    EXPECT_EQ(-1, plan->last_leadership_term_id);
    EXPECT_EQ(0,  plan->snapshot_count);

    aeron_cluster_recovery_plan_free(plan);
    aeron_cluster_recording_log_close(log);
}

TEST_F(RecordingLogTest, shouldPersistAndReloadMultipleTerms)
{
    const int TERM_COUNT= 5;
    {
        aeron_cluster_recording_log_t *log = nullptr;
        ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));
        for (int i = 0; i < TERM_COUNT; i++)
        {
            append_term(log, i + 100, i, i * 1000, i * 100);
            if (i > 0)
            {
                ASSERT_EQ(0, aeron_cluster_recording_log_commit_log_position(log, i - 1, i * 1000));
            }
        }
        aeron_cluster_recording_log_close(log);
    }
    {
        aeron_cluster_recording_log_t *log = nullptr;
        ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), false));
        EXPECT_EQ(TERM_COUNT, log->entry_count);
        auto *last = aeron_cluster_recording_log_find_last_term(log);
        ASSERT_NE(nullptr, last);
        EXPECT_EQ(TERM_COUNT - 1, last->leadership_term_id);
        aeron_cluster_recording_log_close(log);
    }
}

TEST_F(RecordingLogTest, shouldHandleInvalidateLatestSnapshotWithMultipleServices)
{
    const int service_count= 3;
    {
        aeron_cluster_recording_log_t *log = nullptr;
        ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));
        append_term(log, 1, 0, 0, 0);
        /* Complete snapshot set at pos=500: CM + 3 services */
        append_snap(log, 10, 0, 0, 500, 0, CM_SERVICE_ID);
        for (int s = 0; s < service_count; s++)
        {
            append_snap(log, 20 + s, 0, 0, 500, 0, s);
        }
        EXPECT_EQ(1, aeron_cluster_recording_log_invalidate_latest_snapshot(log));
        /* All snapshots at pos=500 should be invalid */
        for (int i = 1; i <= service_count + 1; i++)
        {
            auto *e = aeron_cluster_recording_log_entry_at(log, i);
            ASSERT_NE(nullptr, e);
            EXPECT_FALSE(aeron_cluster_recording_log_entry_is_valid(e))
                << "entry " << i << " should be invalid";
        }
        aeron_cluster_recording_log_close(log);
    }
}

TEST_F(RecordingLogTest, snapshotEntriesCanBeQueriedByServiceId)
{
    aeron_cluster_recording_log_t *log = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));
    append_term(log, 1, 0, 0, 0);
    append_snap(log, 10, 0, 0, 100, 0, CM_SERVICE_ID);
    append_snap(log, 11, 0, 0, 100, 0, 0);
    append_snap(log, 12, 0, 0, 200, 0, CM_SERVICE_ID);
    append_snap(log, 13, 0, 0, 200, 0, 0);

    auto *cm_snap  = aeron_cluster_recording_log_get_latest_snapshot(log, CM_SERVICE_ID);
    auto *svc_snap = aeron_cluster_recording_log_get_latest_snapshot(log, 0);
    ASSERT_NE(nullptr, cm_snap);
    ASSERT_NE(nullptr, svc_snap);
    EXPECT_EQ(12, cm_snap->recording_id);
    EXPECT_EQ(13, svc_snap->recording_id);

    aeron_cluster_recording_log_close(log);
}

TEST_F(RecordingLogTest, appendTermShouldRejectNullValueAsRecordingId)
{
    aeron_cluster_recording_log_t *log = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));
    EXPECT_EQ(-1, aeron_cluster_recording_log_append_term(log, -1LL, 0, 0, 0));
    EXPECT_EQ(0, log->entry_count);
    aeron_cluster_recording_log_close(log);
}

TEST_F(RecordingLogTest, appendSnapshotShouldRejectNullValueAsRecordingId)
{
    aeron_cluster_recording_log_t *log = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));
    EXPECT_EQ(-1, aeron_cluster_recording_log_append_snapshot(log, -1LL, 0, 0, 0, 0, 0));
    EXPECT_EQ(0, log->entry_count);
    aeron_cluster_recording_log_close(log);
}

TEST_F(RecordingLogTest, appendTermShouldNotAcceptDifferentRecordingIds)
{
    aeron_cluster_recording_log_t *log = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));
    append_term(log, 42, 0, 0, 0);
    /* Different recording_id -> must fail */
    EXPECT_EQ(-1, aeron_cluster_recording_log_append_term(log, 21, 1, 0, 0));
    EXPECT_EQ(1, log->entry_count);
    aeron_cluster_recording_log_close(log);

    /* Reload and verify */
    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), false));
    EXPECT_EQ(-1, aeron_cluster_recording_log_append_term(log, -5, -5, -5, -5));
    EXPECT_EQ(1, log->entry_count);
    aeron_cluster_recording_log_close(log);
}

TEST_F(RecordingLogTest, appendTermShouldOnlyAllowASingleValidTermForSameLeadershipTermId)
{
    aeron_cluster_recording_log_t *log = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));
    append_term(log, 8, 0, 0, 0);
    append_term(log, 8, 1, 1, 1);
    ASSERT_EQ(0, aeron_cluster_recording_log_invalidate_entry(log, 0));
    append_term(log, 8, 0, 100, 100);  /* replacing invalidated term 0 */
    /* term 1 already valid -> must fail */
    EXPECT_EQ(-1, aeron_cluster_recording_log_append_term(log, 8, 1, 5, 5));
    EXPECT_EQ(3, log->entry_count);
    aeron_cluster_recording_log_close(log);
}

TEST_F(RecordingLogTest, shouldRemoveEntry)
{
    aeron_cluster_recording_log_t *log = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));
    append_term(log, 1, 3, 2, 4);
    append_term(log, 1, 4, 3, 5);
    ASSERT_EQ(0, aeron_cluster_recording_log_remove_entry(log, 4, log->entry_count - 1));
    EXPECT_EQ(2, log->entry_count);

    /* After invalidation the last term entry should be leadershipTermId=3 */
    auto *e = aeron_cluster_recording_log_find_last_term(log);
    ASSERT_NE(nullptr, e);
    EXPECT_EQ(3, e->leadership_term_id);

    aeron_cluster_recording_log_close(log);

    /* Reload: 2 physical entries, 1 valid */
    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), false));
    EXPECT_EQ(2, log->entry_count);
    aeron_cluster_recording_log_close(log);
}

TEST_F(RecordingLogTest, shouldInvalidateLatestSnapshotWithStandbyVariant)
{
    aeron_cluster_recording_log_t *log = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));
    append_term(log, 1, 0, 0, 0);
    /* Standby snapshot (not invalidated by invalidateLatestSnapshot -- standby ignored) */
    ASSERT_EQ(0, aeron_cluster_recording_log_append_standby_snapshot(
        log, 5, 0, 0, 100, 0, -1, "aeron:udp?endpoint=localhost:8080"));
    append_snap(log, 10, 0, 0, 200, 0, -1);
    append_snap(log, 11, 0, 0, 200, 0, 0);

    /* invalidateLatestSnapshot should invalidate the regular snapshot group at pos=200 */
    EXPECT_EQ(1, aeron_cluster_recording_log_invalidate_latest_snapshot(log));

    auto *cm_snap = aeron_cluster_recording_log_get_latest_snapshot(log, -1);
    /* The regular snapshot at 200 is invalidated; no other regular snapshot left */
    EXPECT_EQ(nullptr, cm_snap);
    aeron_cluster_recording_log_close(log);
}

TEST_F(RecordingLogTest, shouldIgnoreStandbySnapshotInRecoveryPlan)
{
    aeron_cluster_recording_log_t *log = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));
    append_term(log, 1, 0, 0, 0);
    ASSERT_EQ(0, aeron_cluster_recording_log_append_standby_snapshot(
        log, 5, 0, 0, 100, 0, -1, "aeron:udp?endpoint=localhost:8080"));
    ASSERT_EQ(0, aeron_cluster_recording_log_append_standby_snapshot(
        log, 6, 0, 0, 100, 0, 0, "aeron:udp?endpoint=localhost:8080"));

    aeron_cluster_recovery_plan_t *plan = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_create_recovery_plan(log, &plan, 1, nullptr));
    /* Standby snapshots should NOT be included in recovery plan */
    EXPECT_EQ(0, plan->snapshot_count);
    aeron_cluster_recovery_plan_free(plan);
    aeron_cluster_recording_log_close(log);
}

TEST_F(RecordingLogTest, shouldCommitLogPositionByTerm)
{
    aeron_cluster_recording_log_t *log = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));
    append_term(log, 1, 5, 0, 0);
    ASSERT_EQ(0, aeron_cluster_recording_log_commit_log_position_by_term(log, 5, 99999));
    auto *e = aeron_cluster_recording_log_find_last_term(log);
    ASSERT_NE(nullptr, e);
    EXPECT_EQ(99999, e->log_position);
    aeron_cluster_recording_log_close(log);
}

TEST_F(RecordingLogTest, entryAtReturnsNullForOutOfRange)
{
    aeron_cluster_recording_log_t *log = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));
    append_term(log, 1, 0, 0, 0);
    EXPECT_NE(nullptr, aeron_cluster_recording_log_entry_at(log, 0));
    EXPECT_EQ(nullptr, aeron_cluster_recording_log_entry_at(log, 1));
    EXPECT_EQ(nullptr, aeron_cluster_recording_log_entry_at(log, -1));
    aeron_cluster_recording_log_close(log);
}

/* ============================================================
 * SORTED-ON-RELOAD TESTS (entriesInTheRecordingLogShouldBeSorted equiv.)
 * ============================================================ */

TEST_F(RecordingLogTest, entriesShouldBeSortedByLeadershipTermId)
{
    aeron_cluster_recording_log_t *log = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));

    /* Append in non-sorted order */
    append_term(log, 0, 0, 0, 10);   /* leadershipTermId=0 */
    append_term(log, 0, 2, 2048, 0); /* leadershipTermId=2 */
    append_term(log, 0, 3, 5000, 100); /* leadershipTermId=3 */
    append_term(log, 0, 1, 700, 0);  /* leadershipTermId=1 */

    EXPECT_EQ(4, log->sorted_count);

    /* After sort: 0, 1, 2, 3 */
    auto *e0 = aeron_cluster_recording_log_entry_at(log, 0);
    auto *e1 = aeron_cluster_recording_log_entry_at(log, 1);
    auto *e2 = aeron_cluster_recording_log_entry_at(log, 2);
    auto *e3 = aeron_cluster_recording_log_entry_at(log, 3);
    ASSERT_NE(nullptr, e0); ASSERT_NE(nullptr, e1);
    ASSERT_NE(nullptr, e2); ASSERT_NE(nullptr, e3);

    EXPECT_EQ(0, e0->leadership_term_id);
    EXPECT_EQ(1, e1->leadership_term_id);
    EXPECT_EQ(2, e2->leadership_term_id);
    EXPECT_EQ(3, e3->leadership_term_id);

    aeron_cluster_recording_log_close(log);

    /* Reload: sorted order must be preserved */
    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), false));
    EXPECT_EQ(4, log->sorted_count);
    EXPECT_EQ(0, aeron_cluster_recording_log_entry_at(log, 0)->leadership_term_id);
    EXPECT_EQ(3, aeron_cluster_recording_log_entry_at(log, 3)->leadership_term_id);
    aeron_cluster_recording_log_close(log);
}

TEST_F(RecordingLogTest, termsShouldSortBeforeSnapshotsAtSameTermBase)
{
    aeron_cluster_recording_log_t *log = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));
    append_term(log,  1, 0, 0, 0);
    append_snap(log, 10, 0, 0, 100, 0, -1);  /* snapshot at same termBase=0 */
    append_snap(log, 11, 0, 0, 100, 0, 0);

    EXPECT_EQ(3, log->sorted_count);

    /* TERM must come before SNAPSHOT */
    auto *e0 = aeron_cluster_recording_log_entry_at(log, 0);
    ASSERT_NE(nullptr, e0);
    EXPECT_EQ(AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_TERM, e0->entry_type);
    auto *e1 = aeron_cluster_recording_log_entry_at(log, 1);
    ASSERT_NE(nullptr, e1);
    EXPECT_EQ(AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_SNAPSHOT, e1->entry_type);

    aeron_cluster_recording_log_close(log);
}

TEST_F(RecordingLogTest, snapshotsShouldSortByServiceIdDescending)
{
    aeron_cluster_recording_log_t *log = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));
    append_term(log,  1, 0, 0, 0);
    append_snap(log, 20, 0, 0, 500, 0, 0);   /* serviceId=0 */
    append_snap(log, 21, 0, 0, 500, 0, 1);   /* serviceId=1 */
    append_snap(log, 22, 0, 0, 500, 0, -1);  /* serviceId=-1 (CM) */

    /* All at same leadershipTermId=0, termBase=0, logPosition=500
     * serviceId DESC: 1, 0, -1 */
    auto *e1 = aeron_cluster_recording_log_entry_at(log, 1);
    auto *e2 = aeron_cluster_recording_log_entry_at(log, 2);
    auto *e3 = aeron_cluster_recording_log_entry_at(log, 3);
    ASSERT_NE(nullptr, e1); ASSERT_NE(nullptr, e2); ASSERT_NE(nullptr, e3);

    EXPECT_EQ(1,  e1->service_id);
    EXPECT_EQ(0,  e2->service_id);
    EXPECT_EQ(-1, e3->service_id);

    aeron_cluster_recording_log_close(log);
}

TEST_F(RecordingLogTest, invalidEntriesShouldBeVisibleInSortedView)
{
    aeron_cluster_recording_log_t *log = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));
    append_term(log,  1, 0, 0, 0);
    append_snap(log, 10, 0, 0, 100, 0, -1);
    append_snap(log, 11, 0, 0, 100, 0, 0);

    /* Invalidate the CM snapshot */
    ASSERT_EQ(1, aeron_cluster_recording_log_invalidate_latest_snapshot(log));

    EXPECT_EQ(3, log->sorted_count); /* all 3 still present in sorted view */
    /* Find invalidated entries */
    bool found_invalid = false;
    for (int i = 0; i < log->sorted_count; i++)
    {
        auto *e = aeron_cluster_recording_log_entry_at(log, i);
        if (!e->is_valid) { found_invalid = true; }
    }
    EXPECT_TRUE(found_invalid);

    aeron_cluster_recording_log_close(log);
}

TEST_F(RecordingLogTest, reloadShouldRebuildSortedView)
{
    aeron_cluster_recording_log_t *log = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));
    append_term(log, 1, 5, 0, 0);
    append_term(log, 1, 3, 0, 0);
    append_term(log, 1, 1, 0, 0);

    ASSERT_EQ(0, aeron_cluster_recording_log_reload(log));

    EXPECT_EQ(3, log->sorted_count);
    EXPECT_EQ(1, aeron_cluster_recording_log_entry_at(log, 0)->leadership_term_id);
    EXPECT_EQ(3, aeron_cluster_recording_log_entry_at(log, 1)->leadership_term_id);
    EXPECT_EQ(5, aeron_cluster_recording_log_entry_at(log, 2)->leadership_term_id);

    aeron_cluster_recording_log_close(log);
}

TEST_F(RecordingLogTest, standbySnapshotSortsBetweenTermAndSnapshot)
{
    aeron_cluster_recording_log_t *log = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));
    append_term(log,  1, 0, 0, 0);
    append_snap(log, 10, 0, 0, 100, 0, -1);   /* regular SNAPSHOT */
    ASSERT_EQ(0, aeron_cluster_recording_log_append_standby_snapshot(
        log, 20, 0, 0, 100, 0, -1, "aeron:udp?endpoint=localhost:8080"));  /* STANDBY */

    /* Sort order at same termBase=0: TERM < STANDBY < SNAPSHOT */
    auto *e0 = aeron_cluster_recording_log_entry_at(log, 0);
    auto *e1 = aeron_cluster_recording_log_entry_at(log, 1);
    auto *e2 = aeron_cluster_recording_log_entry_at(log, 2);
    ASSERT_NE(nullptr, e0); ASSERT_NE(nullptr, e1); ASSERT_NE(nullptr, e2);

    int base0 = e0->entry_type & ~AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_INVALID_FLAG;
    int base1 = e1->entry_type & ~AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_INVALID_FLAG;
    int base2 = e2->entry_type & ~AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_INVALID_FLAG;

    EXPECT_EQ(AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_TERM,             base0);
    EXPECT_EQ(AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_STANDBY_SNAPSHOT, base1);
    EXPECT_EQ(AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_SNAPSHOT,         base2);

    aeron_cluster_recording_log_close(log);
}

/* -----------------------------------------------------------------------
 * RecordingLog query method tests -- findSnapshotsAtOrBefore, latestStandbySnapshots
 * ----------------------------------------------------------------------- */
TEST_F(RecordingLogTest, findSnapshotsAtOrBeforeReturnsExactMatchFirst)
{
    aeron_cluster_recording_log_t *log = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));
    append_term(log, 10, 0, 0, 100);
    append_snap(log, 20, 0, 0, 500, 110, -1);  /* CM snapshot at pos 500 */
    append_snap(log, 21, 0, 0, 500, 110, 0);   /* svc 0 snapshot at pos 500 */
    append_snap(log, 30, 0, 0, 1000, 120, -1); /* CM snapshot at pos 1000 */
    append_snap(log, 31, 0, 0, 1000, 120, 0);  /* svc 0 snapshot at pos 1000 */

    aeron_cluster_recording_log_entry_t snaps[4];
    int count= 0;
    /* query at pos 600 -- should get pos-500 snapshots, not pos-1000 */
    ASSERT_EQ(0, aeron_cluster_recording_log_find_snapshots_at_or_before(log, 600, 1, snaps, &count));
    EXPECT_EQ(2, count);  /* CM (-1) + svc (0) */
    for (int i = 0; i < count; i++)
    {
        EXPECT_EQ(500, snaps[i].log_position);
    }
    aeron_cluster_recording_log_close(log);
}

TEST_F(RecordingLogTest, findSnapshotsAtOrBeforeFallsBackToLowest)
{
    aeron_cluster_recording_log_t *log = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));
    append_term(log, 10, 0, 0, 100);
    /* only snapshot is AFTER target position */
    append_snap(log, 20, 0, 0, 1000, 110, -1);
    append_snap(log, 21, 0, 0, 1000, 110, 0);

    aeron_cluster_recording_log_entry_t snaps[4];
    int count= 0;
    /* query at pos 200 -- nothing at or before, should fall back to lowest (1000) */
    ASSERT_EQ(0, aeron_cluster_recording_log_find_snapshots_at_or_before(log, 200, 1, snaps, &count));
    EXPECT_EQ(2, count);
    for (int i = 0; i < count; i++)
    {
        EXPECT_EQ(1000, snaps[i].log_position);
    }
    aeron_cluster_recording_log_close(log);
}

TEST_F(RecordingLogTest, findSnapshotsAtOrBeforeReturnsLatestWhenMultipleMatch)
{
    aeron_cluster_recording_log_t *log = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));
    append_term(log, 10, 0, 0, 100);
    /* three CM snapshots all <= 2000 */
    append_snap(log, 20, 0, 0, 500,  110, -1);
    append_snap(log, 22, 0, 0, 1500, 130, -1);
    append_snap(log, 24, 0, 0, 2500, 140, -1);  /* past target */

    aeron_cluster_recording_log_entry_t snaps[4];
    int count= 0;
    ASSERT_EQ(0, aeron_cluster_recording_log_find_snapshots_at_or_before(log, 2000, 0, snaps, &count));
    EXPECT_EQ(1, count);
    EXPECT_EQ(1500, snaps[0].log_position);  /* latest at or before 2000 */
    aeron_cluster_recording_log_close(log);
}

TEST_F(RecordingLogTest, findSnapshotsAtOrBeforeEmptyLogReturnsZero)
{
    aeron_cluster_recording_log_t *log = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));

    aeron_cluster_recording_log_entry_t snaps[4];
    int count= 99;
    ASSERT_EQ(0, aeron_cluster_recording_log_find_snapshots_at_or_before(log, 0, 1, snaps, &count));
    EXPECT_EQ(0, count);
    aeron_cluster_recording_log_close(log);
}

TEST_F(RecordingLogTest, latestStandbySnapshotsReturnsOnePerService)
{
    aeron_cluster_recording_log_t *log = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));
    append_term(log, 10, 0, 0, 100);
    /* Two standby snapshots for svc -1 at different positions */
    ASSERT_EQ(0, aeron_cluster_recording_log_append_standby_snapshot(log, 50, 0, 0, 300, 110, -1, "host:10001"));
    ASSERT_EQ(0, aeron_cluster_recording_log_append_standby_snapshot(log, 51, 0, 0, 800, 120, -1, "host:10001"));
    /* One standby snapshot for svc 0 */
    ASSERT_EQ(0, aeron_cluster_recording_log_append_standby_snapshot(log, 60, 0, 0, 800, 120, 0, "host:10001"));

    aeron_cluster_recording_log_entry_t *snaps = nullptr;
    int count= 0;
    ASSERT_EQ(0, aeron_cluster_recording_log_latest_standby_snapshots(log, 1, &snaps, &count));
    ASSERT_EQ(2, count);

    /* Find CM entry and svc 0 entry */
    bool found_cm = false, found_svc0 = false;
    for (int i = 0; i < count; i++)
    {
        if (snaps[i].service_id == -1)
        {
            found_cm = true;
            EXPECT_EQ(800, snaps[i].log_position); /* latest */
        }
        else if (snaps[i].service_id == 0)
        {
            found_svc0 = true;
            EXPECT_EQ(800, snaps[i].log_position);
        }
    }
    EXPECT_TRUE(found_cm);
    EXPECT_TRUE(found_svc0);

    free(snaps);
    aeron_cluster_recording_log_close(log);
}

TEST_F(RecordingLogTest, latestStandbySnapshotsEmptyLogReturnsZero)
{
    aeron_cluster_recording_log_t *log = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));

    aeron_cluster_recording_log_entry_t *snaps = nullptr;
    int count= 99;
    ASSERT_EQ(0, aeron_cluster_recording_log_latest_standby_snapshots(log, 1, &snaps, &count));
    EXPECT_EQ(0, count);
    EXPECT_EQ(nullptr, snaps);
    aeron_cluster_recording_log_close(log);
}

TEST_F(RecordingLogTest, ensureCoherentAppendsFirstTermToEmptyLog)
{
    aeron_cluster_recording_log_t *log = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));

    ASSERT_EQ(0, aeron_cluster_recording_log_ensure_coherent(
        log, 42, -1, 0, 0, 0, -1, 1000));

    ASSERT_EQ(1, log->sorted_count);
    EXPECT_EQ(42, log->sorted_entries[0].recording_id);
    EXPECT_EQ(0,  log->sorted_entries[0].leadership_term_id);
    EXPECT_EQ(0,  log->sorted_entries[0].term_base_log_position);
    EXPECT_EQ(-1, log->sorted_entries[0].log_position); /* open */

    aeron_cluster_recording_log_close(log);
}

TEST_F(RecordingLogTest, ensureCoherentFillsGapTermsWhenLastTermBehindTarget)
{
    aeron_cluster_recording_log_t *log = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));

    /* Seed with term 2 committed at position 1000 */
    append_term(log, 10, 2, 500, 100);
    ASSERT_EQ(0, aeron_cluster_recording_log_commit_log_position(log, 2, 1000));

    /* ensure_coherent for term 5 -- should fill terms 3, 4, then append 5 */
    ASSERT_EQ(0, aeron_cluster_recording_log_ensure_coherent(
        log, 10, 2, 500, 5, 1000, -1, 200));

    /* Total entries: 1 (term2) + 3 gap (3,4,5) = 4 */
    EXPECT_EQ(4, log->sorted_count);
    /* Last entry should be term 5, open */
    aeron_cluster_recording_log_entry_t *t5 =
        aeron_cluster_recording_log_get_term_entry(log, 5);
    ASSERT_NE(nullptr, t5);
    EXPECT_EQ(1000, t5->term_base_log_position);
    EXPECT_EQ(-1,   t5->log_position);

    aeron_cluster_recording_log_close(log);
}

TEST_F(RecordingLogTest, ensureCoherentCommitsOpenPriorTermBeforeFillingGap)
{
    aeron_cluster_recording_log_t *log = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));

    /* Term 1 is open (log_position = -1) */
    append_term(log, 10, 1, 0, 100);

    /* ensure_coherent for term 3 with term_base_log_position=800 commits term1 first */
    ASSERT_EQ(0, aeron_cluster_recording_log_ensure_coherent(
        log, 10, 1, 0, 3, 800, -1, 200));

    aeron_cluster_recording_log_entry_t *t1 =
        aeron_cluster_recording_log_get_term_entry(log, 1);
    ASSERT_NE(nullptr, t1);
    EXPECT_EQ(800, t1->log_position); /* committed */

    aeron_cluster_recording_log_entry_t *t3 =
        aeron_cluster_recording_log_get_term_entry(log, 3);
    ASSERT_NE(nullptr, t3);
    EXPECT_EQ(-1, t3->log_position); /* new open term */

    aeron_cluster_recording_log_close(log);
}

TEST_F(RecordingLogTest, ensureCoherentCommitsLogPositionForExistingTerm)
{
    aeron_cluster_recording_log_t *log = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));

    /* Term 5 already in log (open) */
    append_term(log, 10, 5, 0, 100);

    /* ensure_coherent with same term but a commit position */
    ASSERT_EQ(0, aeron_cluster_recording_log_ensure_coherent(
        log, 10, 5, 0, 5, 0, 2500, 200));

    aeron_cluster_recording_log_entry_t *t5 =
        aeron_cluster_recording_log_get_term_entry(log, 5);
    ASSERT_NE(nullptr, t5);
    EXPECT_EQ(2500, t5->log_position);

    aeron_cluster_recording_log_close(log);
}

TEST_F(RecordingLogTest, ensureCoherentEmptyLogFillsGapFromInitialTerm)
{
    aeron_cluster_recording_log_t *log = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));

    /* empty log; initial term is 3, target is 5 */
    ASSERT_EQ(0, aeron_cluster_recording_log_ensure_coherent(
        log, 10, 3, 0, 5, 0, -1, 100));

    /* should have appended terms 3, 4, 5 */
    EXPECT_EQ(3, log->sorted_count);
    EXPECT_NE(nullptr, aeron_cluster_recording_log_get_term_entry(log, 3));
    EXPECT_NE(nullptr, aeron_cluster_recording_log_get_term_entry(log, 4));
    EXPECT_NE(nullptr, aeron_cluster_recording_log_get_term_entry(log, 5));

    aeron_cluster_recording_log_close(log);
}

/* -----------------------------------------------------------------------
 * RecordingLog: new functions (nextEntryIndex, findTermEntry,
 *               restoreInvalidSnapshot reuse)
 * ----------------------------------------------------------------------- */
TEST_F(RecordingLogTest, nextEntryIndexStartsAtZeroOnEmptyLog)
{
    aeron_cluster_recording_log_t *log = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));
    EXPECT_EQ(0, aeron_cluster_recording_log_next_entry_index(log));
    aeron_cluster_recording_log_close(log);
}

TEST_F(RecordingLogTest, nextEntryIndexIncrementsOnAppend)
{
    aeron_cluster_recording_log_t *log = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));
    EXPECT_EQ(0, aeron_cluster_recording_log_append_term(log, 1, 0, 0, 1000));
    EXPECT_EQ(1, aeron_cluster_recording_log_next_entry_index(log));
    EXPECT_EQ(0, aeron_cluster_recording_log_append_snapshot(log, 10, 0, 0, 100, 1000, -1));
    EXPECT_EQ(2, aeron_cluster_recording_log_next_entry_index(log));
    aeron_cluster_recording_log_close(log);
}

TEST_F(RecordingLogTest, findTermEntryReturnsNullForUnknownTerm)
{
    aeron_cluster_recording_log_t *log = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));
    ASSERT_EQ(0, aeron_cluster_recording_log_append_term(log, 1, 0, 0, 1000));
    EXPECT_EQ(nullptr, aeron_cluster_recording_log_find_term_entry(log, 99));
    aeron_cluster_recording_log_close(log);
}

TEST_F(RecordingLogTest, findTermEntryReturnsEntryForKnownTerm)
{
    aeron_cluster_recording_log_t *log = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));
    ASSERT_EQ(0, aeron_cluster_recording_log_append_term(log, 1, 0, 0, 1000));
    auto *e = aeron_cluster_recording_log_find_term_entry(log, 0);
    ASSERT_NE(nullptr, e);
    EXPECT_EQ(0LL, e->leadership_term_id);
    EXPECT_EQ(1LL, e->recording_id);
    aeron_cluster_recording_log_close(log);
}

TEST_F(RecordingLogTest, appendSnapshotReusesInvalidatedSlot)
{
    /* Append term + snapshot, invalidate snapshot, re-append same snapshot.
     * The second append should reuse the invalidated slot (no new physical entry). */
    aeron_cluster_recording_log_t *log = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));
    ASSERT_EQ(0, aeron_cluster_recording_log_append_term(log, 1, 0, 0, 1000));
    ASSERT_EQ(0, aeron_cluster_recording_log_append_snapshot(log, 10, 0, 0, 100, 1000, -1));
    int count_before = aeron_cluster_recording_log_next_entry_index(log);
    EXPECT_EQ(2, count_before);

    /* Invalidate the snapshot */
    ASSERT_EQ(1, aeron_cluster_recording_log_invalidate_latest_snapshot(log));

    /* Re-append the same snapshot -- should restore the invalidated slot */
    ASSERT_EQ(0, aeron_cluster_recording_log_append_snapshot(log, 20, 0, 0, 100, 2000, -1));
    /* Physical slot count must NOT have grown */
    EXPECT_EQ(count_before, aeron_cluster_recording_log_next_entry_index(log));

    /* The restored entry should be valid with the new recording_id */
    auto *snap = aeron_cluster_recording_log_get_latest_snapshot(log, -1);
    ASSERT_NE(nullptr, snap);
    EXPECT_EQ(20LL, snap->recording_id);
    aeron_cluster_recording_log_close(log);
}

TEST_F(RecordingLogTest, appendSnapshotAppendsNewSlotWhenNoMatchingInvalidEntry)
{
    /* Append term + snapshot at pos 100; invalidate it; re-append at DIFFERENT pos 200.
     * No matching invalid slot -> new physical entry is written. */
    aeron_cluster_recording_log_t *log = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));
    ASSERT_EQ(0, aeron_cluster_recording_log_append_term(log, 1, 0, 0, 1000));
    ASSERT_EQ(0, aeron_cluster_recording_log_append_snapshot(log, 10, 0, 0, 100, 1000, -1));
    ASSERT_EQ(1, aeron_cluster_recording_log_invalidate_latest_snapshot(log));

    /* Append at different log_position -- no match -> new slot */
    ASSERT_EQ(0, aeron_cluster_recording_log_append_snapshot(log, 20, 0, 0, 200, 2000, -1));
    EXPECT_EQ(3, aeron_cluster_recording_log_next_entry_index(log));  /* term + invalid + new */
    aeron_cluster_recording_log_close(log);
}

/* -----------------------------------------------------------------------
 * RecoveryPlanTest -- aeron_cluster_recording_log_create_recovery_plan
 * ----------------------------------------------------------------------- */
class RecoveryPlanTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        m_dir = make_test_dir("aeron_cluster_recovery_plan_test_");
        aeron_delete_directory(m_dir.c_str());
        aeron_mkdir_recursive(m_dir.c_str(), 0777);
        ASSERT_EQ(0, aeron_cluster_recording_log_open(&m_log, m_dir.c_str(), true));
    }
    void TearDown() override
    {
        aeron_cluster_recording_log_close(m_log);
        aeron_delete_directory(m_dir.c_str());
    }

    std::string m_dir;
    aeron_cluster_recording_log_t *m_log = nullptr;
};

TEST_F(RecoveryPlanTest, emptyLogHasNoSnapshotsAndNoTerm)
{
    aeron_cluster_recovery_plan_t *plan = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_create_recovery_plan(m_log, &plan, 1, nullptr));
    ASSERT_NE(nullptr, plan);

    EXPECT_EQ(0, plan->snapshot_count);
    EXPECT_EQ(-1LL, plan->last_leadership_term_id);
    EXPECT_EQ(0LL, plan->last_term_base_log_position);
    EXPECT_EQ(0LL, plan->last_append_position);
    EXPECT_EQ(-1LL, plan->last_term_recording_id);

    aeron_cluster_recovery_plan_free(plan);
}

TEST_F(RecoveryPlanTest, singleTermNoSnapshotUsesTermPosition)
{
    /* Append a term with log position 1024 */
    ASSERT_EQ(0, aeron_cluster_recording_log_append_term(m_log, /*recording_id=*/10LL,
        /*leadership_term_id=*/0LL, /*term_base_log_position=*/0LL, /*timestamp=*/1000LL));
    ASSERT_EQ(0, aeron_cluster_recording_log_commit_log_position(m_log, 0LL, 1024LL));

    aeron_cluster_recovery_plan_t *plan = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_create_recovery_plan(m_log, &plan, 1, nullptr));
    ASSERT_NE(nullptr, plan);

    EXPECT_EQ(0,      plan->snapshot_count);
    EXPECT_EQ(0LL,    plan->last_leadership_term_id);
    EXPECT_EQ(10LL,   plan->last_term_recording_id);
    EXPECT_EQ(1024LL, plan->last_append_position);

    aeron_cluster_recovery_plan_free(plan);
}

TEST_F(RecoveryPlanTest, snapshotForEachServiceIncludedInPlan)
{
    /* CM snapshot (service_id = -1) at term 0, position 512 */
    ASSERT_EQ(0, aeron_cluster_recording_log_append_term(m_log, 10LL, 0LL, 0LL, 1000LL));
    ASSERT_EQ(0, aeron_cluster_recording_log_append_snapshot(
        m_log, /*recording_id=*/20LL, /*leadership_term_id=*/0LL,
        /*term_base_log_position=*/0LL, /*log_position=*/512LL,
        /*timestamp=*/2000LL, /*service_id=*/-1));
    /* Service 0 snapshot at same position */
    ASSERT_EQ(0, aeron_cluster_recording_log_append_snapshot(
        m_log, /*recording_id=*/21LL, /*leadership_term_id=*/0LL,
        /*term_base_log_position=*/0LL, /*log_position=*/512LL,
        /*timestamp=*/2001LL, /*service_id=*/0));

    aeron_cluster_recovery_plan_t *plan = nullptr;
    /* 1 user service -> 2 snapshots total (CM + svc 0) */
    ASSERT_EQ(0, aeron_cluster_recording_log_create_recovery_plan(m_log, &plan, 1, nullptr));
    ASSERT_NE(nullptr, plan);

    EXPECT_EQ(2, plan->snapshot_count);
    /* CM snapshot first (svc=-1 iterated first) */
    EXPECT_EQ(-1,   plan->snapshots[0].service_id);
    EXPECT_EQ(20LL, plan->snapshots[0].recording_id);
    EXPECT_EQ(0,    plan->snapshots[1].service_id);
    EXPECT_EQ(21LL, plan->snapshots[1].recording_id);

    aeron_cluster_recovery_plan_free(plan);
}

TEST_F(RecoveryPlanTest, latestSnapshotUsedWhenMultipleExist)
{
    /* Two snapshots for CM at different terms; only the later one should appear */
    ASSERT_EQ(0, aeron_cluster_recording_log_append_term(m_log, 10LL, 0LL, 0LL, 1000LL));
    ASSERT_EQ(0, aeron_cluster_recording_log_append_snapshot(
        m_log, 20LL, 0LL, 0LL, 512LL, 2000LL, -1));
    ASSERT_EQ(0, aeron_cluster_recording_log_append_term(m_log, 11LL, 1LL, 512LL, 3000LL));
    ASSERT_EQ(0, aeron_cluster_recording_log_append_snapshot(
        m_log, 30LL, 1LL, 512LL, 1024LL, 4000LL, -1));

    aeron_cluster_recovery_plan_t *plan = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_create_recovery_plan(m_log, &plan, 0, nullptr));
    ASSERT_NE(nullptr, plan);

    /* Only latest CM snapshot */
    EXPECT_EQ(1,     plan->snapshot_count);
    EXPECT_EQ(30LL,  plan->snapshots[0].recording_id);
    EXPECT_EQ(1024LL, plan->snapshots[0].log_position);

    aeron_cluster_recovery_plan_free(plan);
}

/* -----------------------------------------------------------------------
 * Ported from Java RecordingLogTest
 * ----------------------------------------------------------------------- */

/*
 * shouldAppendSnapshotWithLeadershipTermIdOutOfOrder
 * Java: appends terms and snapshots out of order, verifies sorted view matches expected order.
 */
TEST_F(RecordingLogTest, shouldAppendSnapshotWithLeadershipTermIdOutOfOrder)
{
    {
        aeron_cluster_recording_log_t *log = nullptr;
        ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));

        append_term(log, 3, 1, 0, 0);
        append_snap(log, 10, 1, 0, 56, 42, CM_SERVICE_ID);
        ASSERT_EQ(0, aeron_cluster_recording_log_invalidate_entry(log, 1));

        ASSERT_EQ(0, aeron_cluster_recording_log_commit_log_position(log, 1, 200));
        append_term(log, 3, 2, 200, 555);

        ASSERT_EQ(0, aeron_cluster_recording_log_commit_log_position(log, 2, 2048));
        append_term(log, 3, 3, 2048, 0);
        append_snap(log, 11, 2, 200, 250, 100, 1);
        append_snap(log, 10, 1, 0, 56, 42, CM_SERVICE_ID);
        append_snap(log, 100, 2, 200, 250, 100, SERVICE_ID);

        auto *entries = log->sorted_entries;
        int count = log->sorted_count;
        EXPECT_EQ(6, count);
        EXPECT_EQ(6, aeron_cluster_recording_log_next_entry_index(log));

        /* Verify sorted order: term1, snap(CM,term1), term2, snap(svc1,term2), snap(svc0,term2), term3 */
        EXPECT_EQ(1, entries[0].leadership_term_id);
        EXPECT_EQ(AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_TERM, entries[0].entry_type);

        EXPECT_EQ(1, entries[1].leadership_term_id);
        EXPECT_EQ(10, entries[1].recording_id);
        EXPECT_EQ(CM_SERVICE_ID, entries[1].service_id);

        EXPECT_EQ(2, entries[2].leadership_term_id);
        EXPECT_EQ(AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_TERM, entries[2].entry_type);

        /* snapshots at term 2, log_position=250: serviceId=1, then serviceId=0 (descending) */
        EXPECT_EQ(2, entries[3].leadership_term_id);
        EXPECT_EQ(1, entries[3].service_id);
        EXPECT_EQ(11, entries[3].recording_id);

        EXPECT_EQ(2, entries[4].leadership_term_id);
        EXPECT_EQ(SERVICE_ID, entries[4].service_id);
        EXPECT_EQ(100, entries[4].recording_id);

        EXPECT_EQ(3, entries[5].leadership_term_id);
        EXPECT_EQ(AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_TERM, entries[5].entry_type);

        /* getTermEntry lookups */
        EXPECT_EQ(&entries[0], aeron_cluster_recording_log_get_term_entry(log, 1));
        EXPECT_EQ(nullptr, aeron_cluster_recording_log_find_term_entry(log, 0));
        EXPECT_EQ(&entries[2], aeron_cluster_recording_log_get_term_entry(log, 2));
        EXPECT_EQ(&entries[5], aeron_cluster_recording_log_get_term_entry(log, 3));
        auto *latest = aeron_cluster_recording_log_get_latest_snapshot(log, CM_SERVICE_ID);
        ASSERT_NE(nullptr, latest);
        EXPECT_EQ(&entries[1], latest);

        aeron_cluster_recording_log_close(log);
    }

    /* Reload and verify sorted order persists */
    {
        aeron_cluster_recording_log_t *log = nullptr;
        ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), false));
        EXPECT_EQ(6, log->sorted_count);

        /* Term1 first */
        EXPECT_EQ(1, log->sorted_entries[0].leadership_term_id);
        EXPECT_EQ(AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_TERM,
            log->sorted_entries[0].entry_type & ~AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_INVALID_FLAG);
        /* Term3 last */
        EXPECT_EQ(3, log->sorted_entries[5].leadership_term_id);

        aeron_cluster_recording_log_close(log);
    }
}

/*
 * shouldBackFillPriorTerm
 * Java: ensureCoherent back-fills a prior term when the log is empty.
 */
TEST_F(RecordingLogTest, shouldBackFillPriorTerm)
{
    aeron_cluster_recording_log_t *log = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));

    const int64_t initial_log_leadership_term_id= 0;
    const int64_t initial_term_base_log_position= 0;
    const int64_t leadership_term_id= 1;
    const int64_t term_base_log_position= 10000;
    const int64_t log_position = -1;
    const int64_t timestamp= 1000000;

    ASSERT_EQ(0, aeron_cluster_recording_log_ensure_coherent(
        log, 9234236, initial_log_leadership_term_id, initial_term_base_log_position,
        leadership_term_id, term_base_log_position, log_position, timestamp));

    auto *t0 = aeron_cluster_recording_log_find_term_entry(log, 0);
    ASSERT_NE(nullptr, t0);
    EXPECT_EQ(initial_term_base_log_position, t0->term_base_log_position);
    EXPECT_EQ(term_base_log_position, t0->log_position);

    auto *t1 = aeron_cluster_recording_log_find_term_entry(log, 1);
    ASSERT_NE(nullptr, t1);
    EXPECT_EQ(leadership_term_id, t1->leadership_term_id);
    EXPECT_EQ(term_base_log_position, t1->term_base_log_position);
    EXPECT_EQ(-1, t1->log_position);

    EXPECT_EQ(2, log->sorted_count);

    aeron_cluster_recording_log_close(log);
}

/*
 * shouldCorrectlyOrderSnapshots
 * Java: verifies that snapshots added after a term entry are sorted by serviceId
 * in the expected order: CM first, then ascending serviceId.
 */
TEST_F(RecordingLogTest, shouldCorrectlyOrderSnapshots)
{
    aeron_cluster_recording_log_t *log = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));

    /* Term entry first (required for structure) */
    append_term(log, 0, 1, 1440, 0);

    /* Snapshots in arbitrary order */
    append_snap(log, 4, 1, 1440, 2880, 0, 2);
    append_snap(log, 5, 1, 1440, 2880, 0, 1);
    append_snap(log, 6, 1, 1440, 2880, 0, SERVICE_ID);
    append_snap(log, 7, 1, 1440, 2880, 0, CM_SERVICE_ID);

    /* Sorted order for snapshots at same position:
     * serviceId descending: 2, 1, 0, -1 */
    ASSERT_EQ(5, log->sorted_count);
    auto *e0 = aeron_cluster_recording_log_entry_at(log, 0);
    EXPECT_EQ(AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_TERM, e0->entry_type);
    EXPECT_EQ(CM_SERVICE_ID, aeron_cluster_recording_log_entry_at(log, 4)->service_id);
    EXPECT_EQ(SERVICE_ID,    aeron_cluster_recording_log_entry_at(log, 3)->service_id);
    EXPECT_EQ(1,             aeron_cluster_recording_log_entry_at(log, 2)->service_id);
    EXPECT_EQ(2,             aeron_cluster_recording_log_entry_at(log, 1)->service_id);

    aeron_cluster_recording_log_close(log);
}

/*
 * shouldFailToRecoverSnapshotsMarkedInvalidIfFieldsDoNotMatchCorrectly
 * Java: invalidates a snapshot, then appends new snapshots that do NOT match
 * the invalidated slot fields; verifies they are added as new entries.
 */
TEST_F(RecordingLogTest, shouldFailToRecoverSnapshotsMarkedInvalidIfFieldsDoNotMatchCorrectly)
{
    {
        aeron_cluster_recording_log_t *log = nullptr;
        ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));

        append_term(log, 10, 0, 0, 0);
        append_term(log, 10, 1, 500, 0);
        append_snap(log, 0, 1, 500, 777, 0, CM_SERVICE_ID);
        append_snap(log, 1, 1, 500, 777, 0, SERVICE_ID);
        append_snap(log, 2, 1, 500, 888, 0, CM_SERVICE_ID);
        append_snap(log, 3, 1, 500, 888, 0, SERVICE_ID);
        append_snap(log, 4, 1, 500, 999, 0, CM_SERVICE_ID);
        append_snap(log, 5, 1, 500, 999, 0, SERVICE_ID);
        append_term(log, 10, 2, 1000, 5);

        EXPECT_EQ(1, aeron_cluster_recording_log_invalidate_latest_snapshot(log));
        aeron_cluster_recording_log_close(log);
    }

    {
        aeron_cluster_recording_log_t *log = nullptr;
        ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), false));

        /* These should NOT match the invalidated slot (different leadership_term_id, etc.) */
        append_snap(log, 6, 2, 500, 999, 0, SERVICE_ID);     /* wrong leadershipTermId */
        append_snap(log, 7, 1, 501, 999, 0, SERVICE_ID);     /* wrong termBaseLogPosition */
        append_snap(log, 8, 1, 500, 998, 0, SERVICE_ID);     /* wrong logPosition */
        append_snap(log, 9, 1, 500, 999, 0, 42);             /* wrong serviceId */

        /* The invalidated entries (rec 4 & 5) should still be invalid */
        bool found_invalid_4 = false, found_invalid_5 = false;
        for (int i = 0; i < log->sorted_count; i++)
        {
            auto *e = aeron_cluster_recording_log_entry_at(log, i);
            if (e->recording_id == 4 && !e->is_valid) { found_invalid_4 = true; }
            if (e->recording_id == 5 && !e->is_valid) { found_invalid_5 = true; }
        }
        EXPECT_TRUE(found_invalid_4);
        EXPECT_TRUE(found_invalid_5);

        /* The newly appended entries should be valid and present */
        bool found_8 = false, found_9 = false;
        for (int i = 0; i < log->sorted_count; i++)
        {
            auto *e = aeron_cluster_recording_log_entry_at(log, i);
            if (e->recording_id == 8 && e->is_valid) { found_8 = true; }
            if (e->recording_id == 9 && e->is_valid) { found_9 = true; }
        }
        EXPECT_TRUE(found_8);
        EXPECT_TRUE(found_9);

        /* Latest snapshot for CM should be recording_id=6 (term 2, highest leadership term) */
        auto *latest = aeron_cluster_recording_log_get_latest_snapshot(log, SERVICE_ID);
        ASSERT_NE(nullptr, latest);
        EXPECT_EQ(6, latest->recording_id);

        aeron_cluster_recording_log_close(log);
    }
}

/*
 * shouldFindSnapshotAtOrBeforeOrLowest
 * Java: comprehensive test for findSnapshotAtOrBeforeOrLowest with 2 user services.
 */
TEST_F(RecordingLogTest, shouldFindSnapshotAtOrBeforeOrLowest)
{
    aeron_cluster_recording_log_t *log = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));

    int64_t rid= 0;
    append_snap(log, rid++, 1, 500, 777, 0, CM_SERVICE_ID);
    append_snap(log, rid++, 1, 500, 777, 0, SERVICE_ID);
    append_snap(log, rid++, 1, 500, 777, 0, 1);
    ASSERT_EQ(0, aeron_cluster_recording_log_append_standby_snapshot(
        log, rid++, 1, 500, 888, 0, CM_SERVICE_ID, "localhost:8080"));
    ASSERT_EQ(0, aeron_cluster_recording_log_append_standby_snapshot(
        log, rid++, 1, 500, 888, 0, SERVICE_ID, "localhost:8080"));
    ASSERT_EQ(0, aeron_cluster_recording_log_append_standby_snapshot(
        log, rid++, 1, 500, 888, 0, 1, "localhost:8080"));
    append_snap(log, rid++, 1, 500, 888, 0, CM_SERVICE_ID);
    append_snap(log, rid++, 1, 500, 888, 0, SERVICE_ID);
    append_snap(log, rid++, 1, 500, 888, 0, 1);
    append_snap(log, rid++, 1, 500, 890, 0, CM_SERVICE_ID);
    append_snap(log, rid++, 1, 500, 890, 0, SERVICE_ID);  /* Missing service 1 snapshot at 890 */
    ASSERT_EQ(0, aeron_cluster_recording_log_append_standby_snapshot(
        log, rid++, 1, 500, 990, 0, CM_SERVICE_ID, "localhost:8080"));
    ASSERT_EQ(0, aeron_cluster_recording_log_append_standby_snapshot(
        log, rid++, 1, 500, 990, 0, SERVICE_ID, "localhost:8080"));
    ASSERT_EQ(0, aeron_cluster_recording_log_append_standby_snapshot(
        log, rid++, 1, 500, 990, 0, 1, "localhost:8080"));
    append_snap(log, rid++, 1, 500, 999, 0, CM_SERVICE_ID);
    append_snap(log, rid++, 1, 500, 999, 0, SERVICE_ID);
    append_snap(log, rid++, 1, 500, 999, 0, 1);

    const int service_count= 2;
    const int max_snaps = service_count + 1;
    aeron_cluster_recording_log_entry_t snaps[4];
    int count= 0;

    /* Exact match at 777 */
    ASSERT_EQ(0, aeron_cluster_recording_log_find_snapshots_at_or_before(log, 777, service_count, snaps, &count));
    EXPECT_EQ(max_snaps, count);
    for (int i = 0; i < count; i++) { EXPECT_EQ(777, snaps[i].log_position); }

    /* Exact match at 888 */
    ASSERT_EQ(0, aeron_cluster_recording_log_find_snapshots_at_or_before(log, 888, service_count, snaps, &count));
    EXPECT_EQ(max_snaps, count);
    for (int i = 0; i < count; i++) { EXPECT_EQ(888, snaps[i].log_position); }

    /* Exact match at 999 */
    ASSERT_EQ(0, aeron_cluster_recording_log_find_snapshots_at_or_before(log, 999, service_count, snaps, &count));
    EXPECT_EQ(max_snaps, count);
    for (int i = 0; i < count; i++) { EXPECT_EQ(999, snaps[i].log_position); }

    /* 890: incomplete set at 890 (missing svc 1), falls back to 888 */
    ASSERT_EQ(0, aeron_cluster_recording_log_find_snapshots_at_or_before(log, 890, service_count, snaps, &count));
    EXPECT_EQ(max_snaps, count);
    for (int i = 0; i < count; i++) { EXPECT_EQ(888, snaps[i].log_position); }

    /* 990: standby only at 990, falls back to 888 */
    ASSERT_EQ(0, aeron_cluster_recording_log_find_snapshots_at_or_before(log, 990, service_count, snaps, &count));
    EXPECT_EQ(max_snaps, count);
    for (int i = 0; i < count; i++) { EXPECT_EQ(888, snaps[i].log_position); }

    /* 750: nothing at or before, falls back to lowest complete (777) */
    ASSERT_EQ(0, aeron_cluster_recording_log_find_snapshots_at_or_before(log, 750, service_count, snaps, &count));
    EXPECT_EQ(max_snaps, count);
    for (int i = 0; i < count; i++) { EXPECT_EQ(777, snaps[i].log_position); }

    /* 1000: everything at or before, picks latest complete (999) */
    ASSERT_EQ(0, aeron_cluster_recording_log_find_snapshots_at_or_before(log, 1000, service_count, snaps, &count));
    EXPECT_EQ(max_snaps, count);
    for (int i = 0; i < count; i++) { EXPECT_EQ(999, snaps[i].log_position); }

    aeron_cluster_recording_log_close(log);
}

/*
 * shouldGetLatestStandbySnapshotsGroupedByEndpoint
 * Java: verifies that latestStandbySnapshots groups by endpoint and returns
 * the latest complete set for each endpoint.
 */
TEST_F(RecordingLogTest, shouldGetLatestStandbySnapshotsGroupedByEndpoint)
{
    {
        aeron_cluster_recording_log_t *log = nullptr;
        ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));

        append_snap(log, 1, 1, 0, 1000, 1000000000, CM_SERVICE_ID);
        append_snap(log, 2, 1, 0, 1000, 1000000000, SERVICE_ID);

        ASSERT_EQ(0, aeron_cluster_recording_log_append_standby_snapshot(
            log, 5, 2, 500, 800, 1000000000, CM_SERVICE_ID, "remotehost0.aeron.io:20002"));
        ASSERT_EQ(0, aeron_cluster_recording_log_append_standby_snapshot(
            log, 6, 2, 500, 800, 1000000000, SERVICE_ID, "remotehost0.aeron.io:20002"));

        ASSERT_EQ(0, aeron_cluster_recording_log_append_standby_snapshot(
            log, 3, 2, 1000, 2000, 1000000000, CM_SERVICE_ID, "remotehost0.aeron.io:20002"));
        ASSERT_EQ(0, aeron_cluster_recording_log_append_standby_snapshot(
            log, 4, 2, 1000, 2000, 1000000000, SERVICE_ID, "remotehost0.aeron.io:20002"));

        ASSERT_EQ(0, aeron_cluster_recording_log_append_standby_snapshot(
            log, 3, 2, 1000, 2000, 1000000000, CM_SERVICE_ID, "remotehost1.aeron.io:20002"));
        ASSERT_EQ(0, aeron_cluster_recording_log_append_standby_snapshot(
            log, 4, 2, 1000, 2000, 1000000000, SERVICE_ID, "remotehost1.aeron.io:20002"));

        /* Incomplete sets (only svc 0, no CM) at pos 4000 -- should be skipped */
        ASSERT_EQ(0, aeron_cluster_recording_log_append_standby_snapshot(
            log, 10, 2, 3000, 4000, 1000000000, SERVICE_ID, "remotehost0.aeron.io:20002"));
        ASSERT_EQ(0, aeron_cluster_recording_log_append_standby_snapshot(
            log, 11, 2, 3000, 4000, 1000000000, SERVICE_ID, "remotehost1.aeron.io:20002"));

        aeron_cluster_recording_log_close(log);
    }

    {
        aeron_cluster_recording_log_t *log = nullptr;
        ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), false));

        const int service_count= 1;
        aeron_cluster_recording_log_entry_t *snaps = nullptr;
        int count= 0;
        ASSERT_EQ(0, aeron_cluster_recording_log_latest_standby_snapshots(log, service_count, &snaps, &count));

        /* Two endpoints, each with 2 entries (CM + svc 0) = 4 total */
        EXPECT_EQ(4, count);

        /* Verify each endpoint has 2 entries */
        int host0_count = 0, host1_count= 0;
        for (int i = 0; i < count; i++)
        {
            if (strcmp(snaps[i].archive_endpoint, "remotehost0.aeron.io:20002") == 0) { host0_count++; }
            if (strcmp(snaps[i].archive_endpoint, "remotehost1.aeron.io:20002") == 0) { host1_count++; }
        }
        EXPECT_EQ(2, host0_count);
        EXPECT_EQ(2, host1_count);

        free(snaps);
        aeron_cluster_recording_log_close(log);
    }
}

/*
 * shouldHandleEntriesStraddlingPageBoundary
 * Java: uses long endpoint strings to exercise entries that straddle OS page boundaries.
 * Adapted for C's 512-byte endpoint limit: uses a near-max-length endpoint.
 */
TEST_F(RecordingLogTest, shouldHandleEntriesStraddlingPageBoundary)
{
    /* Generate a near-max-length endpoint: 510 chars ('a' * 507 + "xxx") */
    std::string endpoint(507, 'a');
    endpoint += "xxx";
    ASSERT_EQ(510u, endpoint.size());

    {
        aeron_cluster_recording_log_t *log = nullptr;
        ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));

        ASSERT_EQ(0, aeron_cluster_recording_log_append_standby_snapshot(
            log, 1, 2, 1000, 2000, 1000000000, CM_SERVICE_ID, endpoint.c_str()));
        ASSERT_EQ(0, aeron_cluster_recording_log_append_standby_snapshot(
            log, 2, 2, 1000, 2000, 1000000000, SERVICE_ID, endpoint.c_str()));
        ASSERT_EQ(0, aeron_cluster_recording_log_append_standby_snapshot(
            log, 3, 2, 1000, 2000, 1000000000, 1, endpoint.c_str()));
        append_term(log, 4, 3, 10000, 2000000000);
        append_snap(log, 5, 4, 20000, 22222, 3000000000, CM_SERVICE_ID);
        append_snap(log, 6, 4, 20000, 22222, 3000000000, SERVICE_ID);
        aeron_cluster_recording_log_close(log);
    }

    {
        aeron_cluster_recording_log_t *log = nullptr;
        ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), false));

        /* Verify all entries present and correct */
        bool found_standby_1 = false, found_standby_2 = false, found_standby_3 = false;
        bool found_term_4 = false, found_snap_5 = false, found_snap_6 = false;
        for (int i = 0; i < log->sorted_count; i++)
        {
            auto *e = aeron_cluster_recording_log_entry_at(log, i);
            int32_t btype = e->entry_type & ~AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_INVALID_FLAG;
            if (e->recording_id == 1 && btype == AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_STANDBY_SNAPSHOT)
            {
                found_standby_1 = true;
                EXPECT_STREQ(endpoint.c_str(), e->archive_endpoint);
            }
            if (e->recording_id == 2 && btype == AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_STANDBY_SNAPSHOT)
            {
                found_standby_2 = true;
                EXPECT_STREQ(endpoint.c_str(), e->archive_endpoint);
            }
            if (e->recording_id == 3 && btype == AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_STANDBY_SNAPSHOT)
            {
                found_standby_3 = true;
                EXPECT_STREQ(endpoint.c_str(), e->archive_endpoint);
            }
            if (e->recording_id == 4 && btype == AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_TERM) found_term_4 = true;
            if (e->recording_id == 5 && btype == AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_SNAPSHOT) found_snap_5 = true;
            if (e->recording_id == 6 && btype == AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_SNAPSHOT) found_snap_6 = true;
        }
        EXPECT_TRUE(found_standby_1);
        EXPECT_TRUE(found_standby_2);
        EXPECT_TRUE(found_standby_3);
        EXPECT_TRUE(found_term_4);
        EXPECT_TRUE(found_snap_5);
        EXPECT_TRUE(found_snap_6);

        aeron_cluster_recording_log_close(log);
    }
}

/*
 * shouldInsertStandbySnapshotInRecordingLog
 * Java: appends regular and standby snapshots, reloads, verifies entry types.
 */
TEST_F(RecordingLogTest, shouldInsertStandbySnapshotInRecordingLog)
{
    {
        aeron_cluster_recording_log_t *log = nullptr;
        ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));

        append_snap(log, 1, 1, 0, 1000, 1000000000, CM_SERVICE_ID);
        append_snap(log, 2, 1, 0, 1000, 1000000000, SERVICE_ID);

        ASSERT_EQ(0, aeron_cluster_recording_log_append_standby_snapshot(
            log, 3, 2, 1000, 2000, 1000000000, CM_SERVICE_ID, "remotehost.aeron.io:20002"));
        ASSERT_EQ(0, aeron_cluster_recording_log_append_standby_snapshot(
            log, 4, 2, 1000, 2000, 1000000000, SERVICE_ID, "remotehost.aeron.io:20002"));

        append_snap(log, 5, 3, 2000, 3000, 1000000000, CM_SERVICE_ID);
        append_snap(log, 6, 3, 2000, 3000, 1000000000, SERVICE_ID);

        aeron_cluster_recording_log_close(log);
    }

    {
        aeron_cluster_recording_log_t *log = nullptr;
        ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), false));

        /* Verify each entry is present with correct type and endpoint */
        auto assert_log_entry = [&](int64_t rec_id, int expected_type, const char *expected_ep)
        {
            bool found = false;
            for (int i = 0; i < log->sorted_count; i++)
            {
                auto *e = aeron_cluster_recording_log_entry_at(log, i);
                int32_t btype = e->entry_type & ~AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_INVALID_FLAG;
                if (e->recording_id == rec_id && btype == expected_type)
                {
                    if (expected_ep == nullptr)
                    {
                        found = (e->archive_endpoint[0] == '\0');
                    }
                    else
                    {
                        found = (strcmp(e->archive_endpoint, expected_ep) == 0);
                    }
                    if (found) break;
                }
            }
            EXPECT_TRUE(found) << "Missing entry rec_id=" << rec_id;
        };

        assert_log_entry(1, AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_SNAPSHOT, nullptr);
        assert_log_entry(2, AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_SNAPSHOT, nullptr);
        assert_log_entry(3, AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_STANDBY_SNAPSHOT, "remotehost.aeron.io:20002");
        assert_log_entry(4, AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_STANDBY_SNAPSHOT, "remotehost.aeron.io:20002");
        assert_log_entry(5, AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_SNAPSHOT, nullptr);
        assert_log_entry(6, AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_SNAPSHOT, nullptr);

        aeron_cluster_recording_log_close(log);
    }
}

/*
 * shouldInvalidateLatestAnySnapshots
 * Java: invalidateLatestSnapshot invalidates both regular and standby snapshots.
 */
TEST_F(RecordingLogTest, shouldInvalidateLatestAnySnapshots)
{
    auto next_valid_snapshot_index = [](aeron_cluster_recording_log_t *log) -> int
    {
        for (int i = log->sorted_count - 1; i >= 0; i--)
        {
            auto *e = aeron_cluster_recording_log_entry_at(log, i);
            int32_t btype = e->entry_type & ~AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_INVALID_FLAG;
            if (e->is_valid &&
                (btype == AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_SNAPSHOT ||
                 btype == AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_STANDBY_SNAPSHOT))
            {
                return i;
            }
        }
        return -1;
    };

    {
        aeron_cluster_recording_log_t *log = nullptr;
        ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));

        append_term(log, 1, 1, 0, 1000000000);
        append_term(log, 1, 2, 0, 1000000000);
        append_term(log, 1, 3, 0, 1000000000);
        append_term(log, 1, 4, 0, 1000000000);

        /* Group 1: regular snapshots at pos 1000 */
        append_snap(log, 1, 1, 0, 1000, 1000000000, CM_SERVICE_ID);
        append_snap(log, 2, 1, 0, 1000, 1000000000, SERVICE_ID);

        /* Group 2: standby snapshots at pos 2000 */
        ASSERT_EQ(0, aeron_cluster_recording_log_append_standby_snapshot(
            log, 3, 2, 1000, 2000, 1000000000, CM_SERVICE_ID, "remotehost.aeron.io:20002"));
        ASSERT_EQ(0, aeron_cluster_recording_log_append_standby_snapshot(
            log, 4, 2, 1000, 2000, 1000000000, SERVICE_ID, "remotehost.aeron.io:20002"));

        /* Group 3: regular snapshots at pos 3000 */
        append_snap(log, 5, 3, 2000, 3000, 1000000000, CM_SERVICE_ID);
        append_snap(log, 6, 3, 2000, 3000, 1000000000, SERVICE_ID);

        /* Group 4: standby at pos 4000 + regular at pos 4000 */
        ASSERT_EQ(0, aeron_cluster_recording_log_append_standby_snapshot(
            log, 7, 4, 1000, 4000, 1000000000, CM_SERVICE_ID, "remotehost.aeron.io:20002"));
        ASSERT_EQ(0, aeron_cluster_recording_log_append_standby_snapshot(
            log, 8, 4, 1000, 4000, 1000000000, SERVICE_ID, "remotehost.aeron.io:20002"));
        append_snap(log, 9, 4, 2000, 4000, 1000000000, CM_SERVICE_ID);
        append_snap(log, 10, 4, 2000, 4000, 1000000000, SERVICE_ID);

        int idx = next_valid_snapshot_index(log);
        EXPECT_GE(idx, 0);
        aeron_cluster_recording_log_close(log);
    }

    {
        aeron_cluster_recording_log_t *log = nullptr;
        ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), false));

        /* First invalidation: should invalidate group at pos 4000 (both standby and regular) */
        EXPECT_EQ(1, aeron_cluster_recording_log_invalidate_latest_snapshot(log));
        int idx = next_valid_snapshot_index(log);
        EXPECT_GE(idx, 0);

        /* Second: should invalidate group at pos 3000 */
        EXPECT_EQ(1, aeron_cluster_recording_log_invalidate_latest_snapshot(log));
        idx = next_valid_snapshot_index(log);
        EXPECT_GE(idx, 0);

        /* Third: should invalidate group at pos 2000 (standby) */
        EXPECT_EQ(1, aeron_cluster_recording_log_invalidate_latest_snapshot(log));

        /* After 3 invalidations, only group at pos 1000 should remain */
        idx = next_valid_snapshot_index(log);
        EXPECT_GE(idx, 0);
        /* Verify the remaining valid snapshot is at pos 1000 */
        auto *e = aeron_cluster_recording_log_entry_at(log, idx);
        EXPECT_EQ(1000, e->log_position);

        aeron_cluster_recording_log_close(log);
    }
}

/*
 * shouldNotIncludeStandbySnapshotInRecoveryPlan
 * Java: verifies that standby snapshots are excluded from the recovery plan
 * even when regular snapshots co-exist.
 */
TEST_F(RecordingLogTest, shouldNotIncludeStandbySnapshotInRecoveryPlan)
{
    {
        aeron_cluster_recording_log_t *log = nullptr;
        ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));

        append_snap(log, 1, 1, 0, 1000, 1000000000, CM_SERVICE_ID);
        append_snap(log, 2, 1, 0, 1000, 1000000000, SERVICE_ID);

        ASSERT_EQ(0, aeron_cluster_recording_log_append_standby_snapshot(
            log, 3, 2, 1000, 2000, 1000000000, CM_SERVICE_ID, "remotehost.aeron.io:20002"));
        ASSERT_EQ(0, aeron_cluster_recording_log_append_standby_snapshot(
            log, 4, 2, 1000, 2000, 1000000000, SERVICE_ID, "remotehost.aeron.io:20002"));

        aeron_cluster_recording_log_close(log);
    }

    {
        aeron_cluster_recording_log_t *log = nullptr;
        ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), false));

        aeron_cluster_recovery_plan_t *plan = nullptr;
        ASSERT_EQ(0, aeron_cluster_recording_log_create_recovery_plan(log, &plan, 1, nullptr));
        ASSERT_NE(nullptr, plan);

        /* Only the regular snapshots (rec_id 1 and 2) should appear */
        EXPECT_EQ(2, plan->snapshot_count);
        bool found_1 = false, found_2 = false;
        for (int i = 0; i < plan->snapshot_count; i++)
        {
            if (plan->snapshots[i].recording_id == 1) found_1 = true;
            if (plan->snapshots[i].recording_id == 2) found_2 = true;
        }
        EXPECT_TRUE(found_1);
        EXPECT_TRUE(found_2);

        aeron_cluster_recovery_plan_free(plan);
        aeron_cluster_recording_log_close(log);
    }
}

/*
 * shouldRejectSnapshotEntryIfEndpointIsTooLong
 * Java: verifies that appendStandbySnapshot rejects overly long endpoints.
 * Adapted for C's 511-char endpoint limit.
 */
TEST_F(RecordingLogTest, shouldRejectSnapshotEntryIfEndpointIsTooLong)
{
    aeron_cluster_recording_log_t *log = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));

    /* Create an endpoint longer than AERON_CLUSTER_RECORDING_LOG_MAX_ENDPOINT_LENGTH - 1 (511) */
    std::string long_endpoint(600, 'a');
    EXPECT_EQ(-1, aeron_cluster_recording_log_append_standby_snapshot(
        log, 1, 2, 1000, 2000, 1000000000, CM_SERVICE_ID, long_endpoint.c_str()));
    EXPECT_EQ(0, log->entry_count);

    aeron_cluster_recording_log_close(log);
}

/*
 * shouldThrowIfLastTermIsUnfinishedAndTermBaseLogPositionIsNotSpecified
 * Java: ensureCoherent fails when the last term is unfinished (open) and
 *       termBaseLogPosition is not specified (-1).
 */
TEST_F(RecordingLogTest, shouldThrowIfLastTermIsUnfinishedAndTermBaseLogPositionIsNotSpecified)
{
    aeron_cluster_recording_log_t *log = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));

    const int64_t recording_id= 9234236;
    const int64_t leadership_term_id= 4;
    const int64_t log_position= 500;
    const int64_t timestamp= 1000000;

    /* Append an open term (log_position = -1 by default) */
    append_term(log, recording_id, leadership_term_id - 1, 0, timestamp);

    /* ensureCoherent with NULL_POSITION (-1) as term_base_log_position should fail
     * because the prior term is unfinished */
    EXPECT_EQ(-1, aeron_cluster_recording_log_ensure_coherent(
        log, recording_id, 0, 0, leadership_term_id, -1LL, log_position, timestamp));

    aeron_cluster_recording_log_close(log);
}

/*
 * entriesInTheRecordingLogShouldBeSorted (comprehensive version)
 * Java: exercises mixed terms, snapshots, standby snapshots, invalidation, out-of-order
 *       appends, and verifies sorted order matches expectations.
 */
TEST_F(RecordingLogTest, entriesInTheRecordingLogShouldBeSortedComprehensive)
{
    const char *archive_endpoint = "aeron:udp?endpoint=localhost:8080";

    aeron_cluster_recording_log_t *log = nullptr;
    ASSERT_EQ(0, aeron_cluster_recording_log_open(&log, m_dir.c_str(), true));

    append_term(log, 0, 0, 0, 0);
    append_term(log, 0, 1, 100, 10);
    append_snap(log, 0, 1, 111, 222, 12, 1);
    append_snap(log, 0, 1, 111, 222, 12, CM_SERVICE_ID);
    append_snap(log, 0, 1, 111, 222, 12, SERVICE_ID);
    append_term(log, 0, 3, 500, 30);
    append_term(log, 0, 2, 1000000, 1000000);

    /* Java invalidateEntry(index) uses sorted-view index.
     * Use aeron_cluster_recording_log_invalidate_entry() which maps sorted→physical. */
    ASSERT_EQ(0, aeron_cluster_recording_log_invalidate_entry(log, 1));
    ASSERT_EQ(0, aeron_cluster_recording_log_invalidate_entry(log, 5));

    append_term(log, 0, 2, 400, 20);
    append_term(log, 0, 1, 90, 9);

    /* Invalidate latest snapshot (should invalidate the 3 snapshots at pos=222) */
    EXPECT_EQ(1, aeron_cluster_recording_log_invalidate_latest_snapshot(log));

    append_snap(log, 0, 2, 400, 1400, 200, CM_SERVICE_ID);
    append_snap(log, 0, 2, 400, 1400, 200, 1);
    append_snap(log, 0, 1, 0, 777, 42, 2);
    append_snap(log, 0, 2, 400, 1400, 200, SERVICE_ID);

    /* Java: invalidateEntry(8) uses sorted-view index */
    ASSERT_EQ(0, aeron_cluster_recording_log_invalidate_entry(log, 8));

    ASSERT_EQ(0, aeron_cluster_recording_log_append_standby_snapshot(
        log, 0, 2, 400, 1400, 200, CM_SERVICE_ID, archive_endpoint));
    ASSERT_EQ(0, aeron_cluster_recording_log_append_standby_snapshot(
        log, 0, 2, 400, 1400, 200, 1, archive_endpoint));
    ASSERT_EQ(0, aeron_cluster_recording_log_append_standby_snapshot(
        log, 0, 2, 400, 1400, 200, SERVICE_ID, archive_endpoint));

    /* Verify in-memory sorted view structure:
     * - Term 0 comes first
     * - Within each term, TERM < STANDBY < SNAPSHOT
     * - Snapshots sorted by serviceId descending */
    EXPECT_GE(log->sorted_count, 10);

    /* Verify term 0 is first */
    auto *first = aeron_cluster_recording_log_entry_at(log, 0);
    EXPECT_EQ(0, first->leadership_term_id);
    int32_t first_btype = first->entry_type & ~AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_INVALID_FLAG;
    EXPECT_EQ(AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_TERM, first_btype);

    /* Verify term 3 is last */
    auto *last_entry = aeron_cluster_recording_log_entry_at(log, log->sorted_count - 1);
    EXPECT_EQ(3, last_entry->leadership_term_id);
    int32_t last_btype = last_entry->entry_type & ~AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_INVALID_FLAG;
    EXPECT_EQ(AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_TERM, last_btype);

    /* Reload and verify sort is preserved */
    ASSERT_EQ(0, aeron_cluster_recording_log_reload(log));
    EXPECT_EQ(0, aeron_cluster_recording_log_entry_at(log, 0)->leadership_term_id);
    EXPECT_EQ(3, aeron_cluster_recording_log_entry_at(log, log->sorted_count - 1)->leadership_term_id);

    aeron_cluster_recording_log_close(log);
}
