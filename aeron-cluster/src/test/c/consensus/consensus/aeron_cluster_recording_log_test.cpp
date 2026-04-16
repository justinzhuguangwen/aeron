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
        ASSERT_EQ(0, aeron_cluster_recording_log_invalidate_entry_at(log, 2)); /* rec=3 */
        ASSERT_EQ(0, aeron_cluster_recording_log_invalidate_entry_at(log, 3)); /* rec=4 */
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
        ASSERT_EQ(0, aeron_cluster_recording_log_invalidate_entry_at(log, 4));
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
        ASSERT_EQ(0, aeron_cluster_recording_log_invalidate_entry_at(log, 2));
        ASSERT_EQ(0, aeron_cluster_recording_log_invalidate_entry_at(log, 3));
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
    const int TERM_COUNT = 5;
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
    const int service_count = 3;
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
    ASSERT_EQ(0, aeron_cluster_recording_log_invalidate_entry_at(log, 0));
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
    int count = 0;
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
    int count = 0;
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
    int count = 0;
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
    int count = 99;
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
    int count = 0;
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
    int count = 99;
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
