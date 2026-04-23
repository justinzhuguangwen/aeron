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
 * C port of Java LogSourceValidatorTest.
 *
 * Tests verify that aeron_cluster_backup_log_source_is_acceptable()
 * correctly validates whether a given cluster member is an acceptable
 * log source for the configured source type (LEADER, FOLLOWER, ANY).
 */

#include <gtest/gtest.h>
#include <cstdint>

extern "C"
{
#include "aeron_cluster_backup_agent.h"
}

static constexpr int32_t NULL_VALUE = -1;

/* -----------------------------------------------------------------------
 * leaderLogSourceTypeShouldOnlyAcceptLeader
 * ----------------------------------------------------------------------- */
TEST(LogSourceValidatorTest, leaderLogSourceTypeShouldOnlyAcceptLeader)
{
    const int32_t leader_member_id= 123;
    const int32_t follower_member_id= 456;

    EXPECT_TRUE(aeron_cluster_backup_log_source_is_acceptable(
        AERON_CLUSTER_BACKUP_SOURCE_LEADER, leader_member_id, leader_member_id));
    EXPECT_FALSE(aeron_cluster_backup_log_source_is_acceptable(
        AERON_CLUSTER_BACKUP_SOURCE_LEADER, leader_member_id, follower_member_id));
    EXPECT_FALSE(aeron_cluster_backup_log_source_is_acceptable(
        AERON_CLUSTER_BACKUP_SOURCE_LEADER, NULL_VALUE, NULL_VALUE));
    EXPECT_FALSE(aeron_cluster_backup_log_source_is_acceptable(
        AERON_CLUSTER_BACKUP_SOURCE_LEADER, leader_member_id, NULL_VALUE));
    EXPECT_FALSE(aeron_cluster_backup_log_source_is_acceptable(
        AERON_CLUSTER_BACKUP_SOURCE_LEADER, NULL_VALUE, follower_member_id));
}

/* -----------------------------------------------------------------------
 * followerLogSourceTypeShouldOnlyAcceptFollowerAndUnknown
 * ----------------------------------------------------------------------- */
TEST(LogSourceValidatorTest, followerLogSourceTypeShouldOnlyAcceptFollowerAndUnknown)
{
    const int32_t leader_member_id= 123;
    const int32_t follower_member_id= 456;

    EXPECT_FALSE(aeron_cluster_backup_log_source_is_acceptable(
        AERON_CLUSTER_BACKUP_SOURCE_FOLLOWER, leader_member_id, leader_member_id));
    EXPECT_TRUE(aeron_cluster_backup_log_source_is_acceptable(
        AERON_CLUSTER_BACKUP_SOURCE_FOLLOWER, leader_member_id, follower_member_id));
    EXPECT_TRUE(aeron_cluster_backup_log_source_is_acceptable(
        AERON_CLUSTER_BACKUP_SOURCE_FOLLOWER, NULL_VALUE, NULL_VALUE));
    EXPECT_TRUE(aeron_cluster_backup_log_source_is_acceptable(
        AERON_CLUSTER_BACKUP_SOURCE_FOLLOWER, leader_member_id, NULL_VALUE));
    EXPECT_TRUE(aeron_cluster_backup_log_source_is_acceptable(
        AERON_CLUSTER_BACKUP_SOURCE_FOLLOWER, NULL_VALUE, follower_member_id));
}

/* -----------------------------------------------------------------------
 * anyLogSourceTypeShouldAcceptAny
 * ----------------------------------------------------------------------- */
TEST(LogSourceValidatorTest, anyLogSourceTypeShouldAcceptAny)
{
    const int32_t leader_member_id= 123;
    const int32_t follower_member_id= 456;

    EXPECT_TRUE(aeron_cluster_backup_log_source_is_acceptable(
        AERON_CLUSTER_BACKUP_SOURCE_ANY, leader_member_id, leader_member_id));
    EXPECT_TRUE(aeron_cluster_backup_log_source_is_acceptable(
        AERON_CLUSTER_BACKUP_SOURCE_ANY, leader_member_id, follower_member_id));
    EXPECT_TRUE(aeron_cluster_backup_log_source_is_acceptable(
        AERON_CLUSTER_BACKUP_SOURCE_ANY, NULL_VALUE, NULL_VALUE));
    EXPECT_TRUE(aeron_cluster_backup_log_source_is_acceptable(
        AERON_CLUSTER_BACKUP_SOURCE_ANY, leader_member_id, NULL_VALUE));
    EXPECT_TRUE(aeron_cluster_backup_log_source_is_acceptable(
        AERON_CLUSTER_BACKUP_SOURCE_ANY, NULL_VALUE, follower_member_id));
}
