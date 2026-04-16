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
#include <cstdint>

extern "C"
{
#include "aeron_cluster_pending_message_tracker.h"
}

class PendingMessageTrackerTest : public ::testing::Test
{
protected:
    aeron_cluster_pending_message_tracker_t m_tracker{};

    void init(int32_t svc, int64_t next_id, int64_t log_id, int64_t cap = 4096)
    {
        aeron_cluster_pending_message_tracker_init(&m_tracker, svc, next_id, log_id, cap);
    }

    void TearDown() override
    {
        aeron_cluster_pending_message_tracker_close(&m_tracker);
    }
};

TEST_F(PendingMessageTrackerTest, shouldAppendOnlyUncommittedMessages)
{
    init(0, 1, 5);
    EXPECT_FALSE(aeron_cluster_pending_message_tracker_should_append(&m_tracker, 3));
    EXPECT_FALSE(aeron_cluster_pending_message_tracker_should_append(&m_tracker, 5));
    EXPECT_TRUE (aeron_cluster_pending_message_tracker_should_append(&m_tracker, 6));
    EXPECT_TRUE (aeron_cluster_pending_message_tracker_should_append(&m_tracker, 100));
}

TEST_F(PendingMessageTrackerTest, shouldAdvanceNextSessionIdOnAppend)
{
    init(0, 1, 0);
    EXPECT_EQ(1, m_tracker.next_service_session_id);
    aeron_cluster_pending_message_tracker_on_appended(&m_tracker, 3);
    EXPECT_EQ(4, m_tracker.next_service_session_id);
    aeron_cluster_pending_message_tracker_on_appended(&m_tracker, 10);
    EXPECT_EQ(11, m_tracker.next_service_session_id);
}

TEST_F(PendingMessageTrackerTest, shouldNotDecreaseNextSessionIdOnAppend)
{
    init(0, 1, 0);
    aeron_cluster_pending_message_tracker_on_appended(&m_tracker, 10);
    EXPECT_EQ(11, m_tracker.next_service_session_id);
    aeron_cluster_pending_message_tracker_on_appended(&m_tracker, 5);  /* lower ID */
    EXPECT_EQ(11, m_tracker.next_service_session_id);  /* unchanged */
}

TEST_F(PendingMessageTrackerTest, shouldAdvanceLogServiceSessionIdOnCommit)
{
    init(0, 1, 0);
    EXPECT_EQ(0, m_tracker.log_service_session_id);
    aeron_cluster_pending_message_tracker_on_committed(&m_tracker, 7);
    EXPECT_EQ(7, m_tracker.log_service_session_id);
}

TEST_F(PendingMessageTrackerTest, shouldNotDecreaseLogServiceSessionIdOnCommit)
{
    init(0, 1, 10);
    aeron_cluster_pending_message_tracker_on_committed(&m_tracker, 5);
    EXPECT_EQ(10, m_tracker.log_service_session_id);  /* unchanged */
}

TEST_F(PendingMessageTrackerTest, emptyTrackerAllowsAllMessages)
{
    init(0, 1, 0);
    for (int64_t i = 1; i <= 100; i++)
    {
        EXPECT_TRUE(aeron_cluster_pending_message_tracker_should_append(&m_tracker, i));
    }
}

TEST_F(PendingMessageTrackerTest, trackerWithHighLogIdBlocksLowerMessages)
{
    init(0, 50, 100);
    EXPECT_FALSE(aeron_cluster_pending_message_tracker_should_append(&m_tracker, 50));
    EXPECT_FALSE(aeron_cluster_pending_message_tracker_should_append(&m_tracker, 100));
    EXPECT_TRUE (aeron_cluster_pending_message_tracker_should_append(&m_tracker, 101));
}
