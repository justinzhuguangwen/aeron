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
#include <string>
#include <vector>
#include <algorithm>

extern "C"
{
#include "aeron_cluster_publication_group.h"
}

// ---------------------------------------------------------------------------
// Mock publication: a lightweight tag object that records the channel/streamId
// used to create it and tracks whether close() has been called.
// ---------------------------------------------------------------------------
struct MockPublication
{
    std::string channel;
    int32_t stream_id;
    bool closed;
};

// Global registry so the factory can return stable pointers and the test can
// inspect which publications were created.
struct MockRegistry
{
    std::vector<MockPublication *> pubs;

    // Pre-allocated publications keyed by endpoint substring.
    MockPublication pub_1001{"", 0, false};
    MockPublication pub_1002{"", 0, false};
    MockPublication pub_1003{"", 0, false};

    MockPublication *lookup(const char *channel)
    {
        if (strstr(channel, "localhost:1001"))
        {
            return &pub_1001;
        }
        if (strstr(channel, "localhost:1002"))
        {
            return &pub_1002;
        }
        if (strstr(channel, "localhost:1003"))
        {
            return &pub_1003;
        }
        return nullptr;
    }

    void reset()
    {
        pub_1001 = {"", 0, false};
        pub_1002 = {"", 0, false};
        pub_1003 = {"", 0, false};
        pubs.clear();
    }
};

static MockRegistry g_registry;

static void *mock_factory(void *clientd, const char *channel, int32_t stream_id)
{
    (void)clientd;
    MockPublication *pub = g_registry.lookup(channel);
    if (nullptr != pub)
    {
        pub->channel = channel;
        pub->stream_id = stream_id;
        pub->closed = false;
        g_registry.pubs.push_back(pub);
    }
    return pub;
}

static void mock_close(void *publication)
{
    if (nullptr != publication)
    {
        static_cast<MockPublication *>(publication)->closed = true;
    }
}

// ---------------------------------------------------------------------------
// Test fixture
// ---------------------------------------------------------------------------
class PublicationGroupTest : public ::testing::Test
{
protected:
    static constexpr const char *CHANNEL_TEMPLATE = "aeron:udp?term-length=64k";
    enum { STREAM_ID = 10000 };

    const char *m_endpoints[3] = {"localhost:1001", "localhost:1002", "localhost:1003"};
    aeron_cluster_publication_group_t m_group{};

    void SetUp() override
    {
        g_registry.reset();
        ASSERT_EQ(0, aeron_cluster_publication_group_init(
            &m_group,
            m_endpoints, 3,
            CHANNEL_TEMPLATE,
            STREAM_ID,
            mock_factory,
            mock_close));
    }

    void TearDown() override
    {
        aeron_cluster_publication_group_fini(&m_group);
    }
};

// 1. shouldUseAllPublicationsInListWhenGettingNextPublication
TEST_F(PublicationGroupTest, shouldUseAllPublicationsInListWhenGettingNextPublication)
{
    aeron_cluster_publication_group_next(&m_group, nullptr);
    aeron_cluster_publication_group_next(&m_group, nullptr);
    aeron_cluster_publication_group_next(&m_group, nullptr);

    // All three endpoints must have been used, each with the correct stream id.
    EXPECT_FALSE(g_registry.pub_1001.channel.empty());
    EXPECT_FALSE(g_registry.pub_1002.channel.empty());
    EXPECT_FALSE(g_registry.pub_1003.channel.empty());

    // Channels should contain the endpoint.
    EXPECT_NE(std::string::npos, g_registry.pub_1001.channel.find("localhost:1001"));
    EXPECT_NE(std::string::npos, g_registry.pub_1002.channel.find("localhost:1002"));
    EXPECT_NE(std::string::npos, g_registry.pub_1003.channel.find("localhost:1003"));

    // Channels should contain the term-length parameter.
    EXPECT_NE(std::string::npos, g_registry.pub_1001.channel.find("term-length=64k"));
    EXPECT_NE(std::string::npos, g_registry.pub_1002.channel.find("term-length=64k"));
    EXPECT_NE(std::string::npos, g_registry.pub_1003.channel.find("term-length=64k"));

    // Stream IDs must match.
    EXPECT_EQ(STREAM_ID, g_registry.pub_1001.stream_id);
    EXPECT_EQ(STREAM_ID, g_registry.pub_1002.stream_id);
    EXPECT_EQ(STREAM_ID, g_registry.pub_1003.stream_id);
}

// 2. shouldNextPublicationAndReturnSameWithCurrent
TEST_F(PublicationGroupTest, shouldNextPublicationAndReturnSameWithCurrent)
{
    void *pub_next = aeron_cluster_publication_group_next(&m_group, nullptr);
    void *pub_current1 = aeron_cluster_publication_group_current(&m_group);
    void *pub_current2 = aeron_cluster_publication_group_current(&m_group);

    EXPECT_EQ(pub_next, pub_current1);
    EXPECT_EQ(pub_next, pub_current2);

    // Factory should have been called exactly once.
    EXPECT_EQ(1u, g_registry.pubs.size());
}

// 3. shouldResultNullWhenCurrentCalledWithoutNext
TEST_F(PublicationGroupTest, shouldReturnNullWhenCurrentCalledWithoutNext)
{
    EXPECT_EQ(nullptr, aeron_cluster_publication_group_current(&m_group));
}

// 4. shouldCloseWhenCurrentIsNotNullWhenNextIsCalled
TEST_F(PublicationGroupTest, shouldCloseCurrentWhenNextIsCalled)
{
    EXPECT_EQ(nullptr, aeron_cluster_publication_group_current(&m_group));

    void *current = aeron_cluster_publication_group_next(&m_group, nullptr);
    EXPECT_NE(nullptr, current);

    // Calling next again should close the previous publication.
    void *next = aeron_cluster_publication_group_next(&m_group, nullptr);
    EXPECT_NE(nullptr, next);
    EXPECT_TRUE(static_cast<MockPublication *>(current)->closed);
}

// 5. shouldExcludeCurrentPublication
TEST_F(PublicationGroupTest, shouldExcludeCurrentPublication)
{
    void *to_exclude = aeron_cluster_publication_group_next(&m_group, nullptr);
    aeron_cluster_publication_group_close_and_exclude_current(&m_group);

    for (int i = 0; i < 100; i++)
    {
        void *pub = aeron_cluster_publication_group_next(&m_group, nullptr);
        EXPECT_NE(to_exclude, pub);
    }
}

// 6. shouldClearExclusion
TEST_F(PublicationGroupTest, shouldClearExclusion)
{
    void *to_exclude = aeron_cluster_publication_group_next(&m_group, nullptr);
    aeron_cluster_publication_group_close_and_exclude_current(&m_group);

    void *after_exclude = aeron_cluster_publication_group_next(&m_group, nullptr);
    EXPECT_NE(to_exclude, after_exclude);

    // Cycle through all endpoints -- excluded one should not appear.
    for (int i = 0; i < 3; i++)
    {
        EXPECT_NE(to_exclude, aeron_cluster_publication_group_next(&m_group, nullptr));
    }

    aeron_cluster_publication_group_clear_exclusion(&m_group);

    // After clearing exclusion, the previously excluded publication should reappear.
    bool found = false;
    for (int i = 0; i < 3; i++)
    {
        if (to_exclude == aeron_cluster_publication_group_next(&m_group, nullptr))
        {
            found = true;
        }
    }
    EXPECT_TRUE(found);
}

// 7. shouldClosePublicationOnClose
TEST_F(PublicationGroupTest, shouldClosePublicationOnClose)
{
    void *pub = aeron_cluster_publication_group_next(&m_group, nullptr);
    EXPECT_NE(nullptr, pub);

    aeron_cluster_publication_group_close(&m_group);
    EXPECT_TRUE(static_cast<MockPublication *>(pub)->closed);
}

// 8. shouldEventuallyGetADifferentOrderAfterShuffle
TEST_F(PublicationGroupTest, shouldEventuallyGetADifferentOrderAfterShuffle)
{
    // Take a copy of the original endpoint order.
    std::vector<std::string> original_order;
    for (int i = 0; i < m_group.endpoints_length; i++)
    {
        original_order.emplace_back(m_group.endpoints[i]);
    }

    int difference_count= 0;
    for (int i = 0; i < 100; i++)
    {
        aeron_cluster_publication_group_shuffle(&m_group);

        std::vector<std::string> current_order;
        for (int j = 0; j < m_group.endpoints_length; j++)
        {
            current_order.emplace_back(m_group.endpoints[j]);
        }

        if (current_order != original_order)
        {
            difference_count++;
        }
    }

    EXPECT_NE(0, difference_count);
}
