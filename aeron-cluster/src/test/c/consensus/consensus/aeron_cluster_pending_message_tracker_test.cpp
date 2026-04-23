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
#include <cstring>
#include <vector>

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

// ---------------------------------------------------------------------------
// Mock log-append callback: always succeeds, returns a fixed position (64).
// ---------------------------------------------------------------------------
static int64_t mock_log_append(
    void       *clientd,
    int64_t     leadership_term_id,
    int64_t     cluster_session_id,
    int64_t     timestamp,
    uint8_t    *buffer,
    int         payload_offset,
    int         payload_length)
{
    (void)clientd;
    (void)leadership_term_id;
    (void)cluster_session_id;
    (void)timestamp;
    (void)buffer;
    (void)payload_offset;
    (void)payload_length;
    return 64;
}

// ---------------------------------------------------------------------------
// Snapshot helper: captures pending messages and tracker state.
// ---------------------------------------------------------------------------
struct Snapshot
{
    int64_t next_service_session_id;
    int64_t log_service_session_id;
    int     size;
    std::vector<std::vector<uint8_t>> messages;
};

static bool snapshot_consumer(
    void    *clientd,
    uint8_t *buffer,
    int      offset,
    int      length,
    int      head_offset)
{
    (void)head_offset;
    auto *snapshot = static_cast<Snapshot *>(clientd);
    snapshot->messages.emplace_back(buffer + offset, buffer + offset + length);
    return true;
}

static Snapshot take_snapshot(aeron_cluster_pending_message_tracker_t *tracker)
{
    Snapshot snap{};
    snap.next_service_session_id = tracker->next_service_session_id;
    snap.log_service_session_id  = tracker->log_service_session_id;
    snap.size = aeron_cluster_pending_message_tracker_size(tracker);

    aeron_expandable_ring_buffer_for_each(
        &tracker->pending_messages, &snap, snapshot_consumer, INT32_MAX);

    return snap;
}

static void load_snapshot(
    aeron_cluster_pending_message_tracker_t *tracker,
    const Snapshot &snap)
{
    aeron_cluster_pending_message_tracker_load_state(
        tracker, snap.next_service_session_id, snap.log_service_session_id, snap.size);

    for (auto &msg : snap.messages)
    {
        aeron_cluster_pending_message_tracker_append_message(
            tracker, msg.data(), 0, static_cast<int>(msg.size()));
    }
}

// ---------------------------------------------------------------------------
// loadInvalid: loadState where next == log, verify should fail.
// ---------------------------------------------------------------------------
TEST_F(PendingMessageTrackerTest, loadInvalid)
{
    init(0, 1, 0);

    aeron_cluster_pending_message_tracker_load_state(
        &m_tracker, INT64_C(-9223372036854774166), INT64_C(-9223372036854774166), 0);

    EXPECT_EQ(-1, aeron_cluster_pending_message_tracker_verify(&m_tracker));
}

// ---------------------------------------------------------------------------
// loadValid: loadState where next == log + 1, verify should pass.
// ---------------------------------------------------------------------------
TEST_F(PendingMessageTrackerTest, loadValid)
{
    init(0, 1, 0);

    aeron_cluster_pending_message_tracker_load_state(
        &m_tracker, INT64_C(-9223372036854774165), INT64_C(-9223372036854774166), 0);

    EXPECT_EQ(0, aeron_cluster_pending_message_tracker_verify(&m_tracker));
}

// ---------------------------------------------------------------------------
// snapshotEmpty: empty tracker -> snapshot -> load -> verify
// ---------------------------------------------------------------------------
TEST_F(PendingMessageTrackerTest, snapshotEmpty)
{
    init(0, 1, 0);

    Snapshot snap = take_snapshot(&m_tracker);

    aeron_cluster_pending_message_tracker_t loaded{};
    aeron_cluster_pending_message_tracker_init(&loaded, 0, 1, 0, 4096);
    load_snapshot(&loaded, snap);

    EXPECT_EQ(0, aeron_cluster_pending_message_tracker_verify(&loaded));
    aeron_cluster_pending_message_tracker_close(&loaded);
}

// ---------------------------------------------------------------------------
// snapshotAfterEnqueueBeforePollAndSweep
// ---------------------------------------------------------------------------
TEST_F(PendingMessageTrackerTest, snapshotAfterEnqueueBeforePollAndSweep)
{
    init(0, 1, 0);

    uint8_t message[64];
    memset(message, 0, sizeof(message));

    aeron_cluster_pending_message_tracker_enqueue_message(
        &m_tracker, message, AERON_CLUSTER_SESSION_HEADER_LENGTH,
        static_cast<int>(sizeof(message)) - AERON_CLUSTER_SESSION_HEADER_LENGTH);

    Snapshot snap = take_snapshot(&m_tracker);

    aeron_cluster_pending_message_tracker_t loaded{};
    aeron_cluster_pending_message_tracker_init(&loaded, 0, 1, 0, 4096);
    load_snapshot(&loaded, snap);

    EXPECT_EQ(0, aeron_cluster_pending_message_tracker_verify(&loaded));
    aeron_cluster_pending_message_tracker_close(&loaded);
}

// ---------------------------------------------------------------------------
// snapshotAfterEnqueueAndPollBeforeSweep
// ---------------------------------------------------------------------------
TEST_F(PendingMessageTrackerTest, snapshotAfterEnqueueAndPollBeforeSweep)
{
    init(0, 1, 0);

    uint8_t message[64];
    memset(message, 0, sizeof(message));

    aeron_cluster_pending_message_tracker_enqueue_message(
        &m_tracker, message, AERON_CLUSTER_SESSION_HEADER_LENGTH,
        static_cast<int>(sizeof(message)) - AERON_CLUSTER_SESSION_HEADER_LENGTH);

    aeron_cluster_pending_message_tracker_poll(
        &m_tracker, 0, 0, nullptr, mock_log_append);

    Snapshot snap = take_snapshot(&m_tracker);

    aeron_cluster_pending_message_tracker_t loaded{};
    aeron_cluster_pending_message_tracker_init(&loaded, 0, 1, 0, 4096);
    load_snapshot(&loaded, snap);

    EXPECT_EQ(0, aeron_cluster_pending_message_tracker_verify(&loaded));
    aeron_cluster_pending_message_tracker_close(&loaded);
}

// ---------------------------------------------------------------------------
// snapshotAfterEnqueuePollAndSweepForLeader
// ---------------------------------------------------------------------------
TEST_F(PendingMessageTrackerTest, snapshotAfterEnqueuePollAndSweepForLeader)
{
    init(0, 1, 0);

    uint8_t message[64];
    memset(message, 0, sizeof(message));

    aeron_cluster_pending_message_tracker_enqueue_message(
        &m_tracker, message, AERON_CLUSTER_SESSION_HEADER_LENGTH,
        static_cast<int>(sizeof(message)) - AERON_CLUSTER_SESSION_HEADER_LENGTH);

    aeron_cluster_pending_message_tracker_poll(
        &m_tracker, 0, 0, nullptr, mock_log_append);

    // mock_log_append returns 64, so sweeping with commit_position >= 64 commits the message.
    aeron_cluster_pending_message_tracker_sweep_leader_messages(&m_tracker, INT64_MAX);

    Snapshot snap = take_snapshot(&m_tracker);

    aeron_cluster_pending_message_tracker_t loaded{};
    aeron_cluster_pending_message_tracker_init(&loaded, 0, 1, 0, 4096);
    load_snapshot(&loaded, snap);

    EXPECT_EQ(0, aeron_cluster_pending_message_tracker_verify(&loaded));
    aeron_cluster_pending_message_tracker_close(&loaded);
}

// ---------------------------------------------------------------------------
// snapshotAfterEnqueuePollAndSweepForFollower
// ---------------------------------------------------------------------------
TEST_F(PendingMessageTrackerTest, snapshotAfterEnqueuePollAndSweepForFollower)
{
    init(0, 1, 0);

    uint8_t message[64];
    memset(message, 0, sizeof(message));

    aeron_cluster_pending_message_tracker_enqueue_message(
        &m_tracker, message, AERON_CLUSTER_SESSION_HEADER_LENGTH,
        static_cast<int>(sizeof(message)) - AERON_CLUSTER_SESSION_HEADER_LENGTH);

    // Read the cluster session ID stamped by enqueue (at byte 16 of the header).
    int64_t cluster_session_id;
    memcpy(&cluster_session_id, message + AERON_PMT_CLUSTER_SESSION_ID_OFFSET, sizeof(int64_t));

    aeron_cluster_pending_message_tracker_poll(
        &m_tracker, 0, 0, nullptr, mock_log_append);

    aeron_cluster_pending_message_tracker_sweep_follower_messages(
        &m_tracker, cluster_session_id);

    Snapshot snap = take_snapshot(&m_tracker);

    aeron_cluster_pending_message_tracker_t loaded{};
    aeron_cluster_pending_message_tracker_init(&loaded, 0, 1, 0, 4096);
    load_snapshot(&loaded, snap);

    EXPECT_EQ(0, aeron_cluster_pending_message_tracker_verify(&loaded));
    aeron_cluster_pending_message_tracker_close(&loaded);
}
