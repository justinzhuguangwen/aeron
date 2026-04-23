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

#if defined(__linux__)
#define _DEFAULT_SOURCE
#define _GNU_SOURCE
#endif

#include "aeron_cluster_publication_group.h"

#include <stdlib.h>
#include <string.h>

#include "uri/aeron_uri_string_builder.h"
#include "uri/aeron_uri.h"

int aeron_cluster_publication_group_init(
    aeron_cluster_publication_group_t *group,
    const char *const *endpoints,
    int endpoints_length,
    const char *channel_template,
    int32_t stream_id,
    aeron_cluster_publication_group_factory_func_t factory,
    aeron_cluster_publication_group_close_func_t close_func)
{
    if (NULL == group || NULL == endpoints || endpoints_length <= 0 ||
        NULL == channel_template || NULL == factory)
    {
        return -1;
    }

    group->endpoints = (char **)calloc((size_t)endpoints_length, sizeof(char *));
    if (NULL == group->endpoints)
    {
        return -1;
    }

    for (int i = 0; i < endpoints_length; i++)
    {
        group->endpoints[i] = strdup(endpoints[i]);
        if (NULL == group->endpoints[i])
        {
            for (int j = 0; j < i; j++)
            {
                free(group->endpoints[j]);
            }
            free(group->endpoints);
            group->endpoints = NULL;
            return -1;
        }
    }

    group->endpoints_length = endpoints_length;
    group->channel_template = strdup(channel_template);
    if (NULL == group->channel_template)
    {
        for (int i = 0; i < endpoints_length; i++)
        {
            free(group->endpoints[i]);
        }
        free(group->endpoints);
        group->endpoints = NULL;
        return -1;
    }

    group->stream_id = stream_id;
    group->factory = factory;
    group->close_func = close_func;
    group->cursor = 0;
    group->excluded_cursor_value = -1;
    group->current = NULL;

    return 0;
}

void aeron_cluster_publication_group_fini(aeron_cluster_publication_group_t *group)
{
    if (NULL == group)
    {
        return;
    }

    if (NULL != group->endpoints)
    {
        for (int i = 0; i < group->endpoints_length; i++)
        {
            free(group->endpoints[i]);
        }
        free(group->endpoints);
        group->endpoints = NULL;
    }

    free(group->channel_template);
    group->channel_template = NULL;
    group->endpoints_length = 0;
}

static int aeron_cluster_publication_group_next_cursor(aeron_cluster_publication_group_t *group)
{
    do
    {
        ++group->cursor;
        if (group->endpoints_length <= group->cursor)
        {
            group->cursor = 0;
        }
    }
    while (group->cursor == group->excluded_cursor_value);

    return group->cursor;
}

static int aeron_cluster_publication_group_build_channel(
    const aeron_cluster_publication_group_t *group,
    const char *endpoint,
    char *channel,
    size_t channel_len)
{
    aeron_uri_string_builder_t builder;

    if (aeron_uri_string_builder_init_on_string(&builder, group->channel_template) < 0)
    {
        return -1;
    }

    if (aeron_uri_string_builder_put(&builder, AERON_UDP_CHANNEL_ENDPOINT_KEY, endpoint) < 0)
    {
        aeron_uri_string_builder_close(&builder);
        return -1;
    }

    if (aeron_uri_string_builder_sprint(&builder, channel, channel_len) < 0)
    {
        aeron_uri_string_builder_close(&builder);
        return -1;
    }

    aeron_uri_string_builder_close(&builder);
    return 0;
}

void *aeron_cluster_publication_group_next(
    aeron_cluster_publication_group_t *group, void *clientd)
{
    if (NULL == group)
    {
        return NULL;
    }

    const int cursor = aeron_cluster_publication_group_next_cursor(group);
    const char *endpoint = group->endpoints[cursor];

    char channel[512];
    if (aeron_cluster_publication_group_build_channel(group, endpoint, channel, sizeof(channel)) < 0)
    {
        return NULL;
    }

    if (NULL != group->current && NULL != group->close_func)
    {
        group->close_func(group->current);
    }

    group->current = group->factory(clientd, channel, group->stream_id);
    return group->current;
}

void *aeron_cluster_publication_group_current(
    const aeron_cluster_publication_group_t *group)
{
    if (NULL == group)
    {
        return NULL;
    }
    return group->current;
}

void aeron_cluster_publication_group_close_and_exclude_current(
    aeron_cluster_publication_group_t *group)
{
    if (NULL == group)
    {
        return;
    }

    group->excluded_cursor_value = group->cursor;

    if (NULL != group->current && NULL != group->close_func)
    {
        group->close_func(group->current);
    }
}

void aeron_cluster_publication_group_clear_exclusion(
    aeron_cluster_publication_group_t *group)
{
    if (NULL == group)
    {
        return;
    }
    group->excluded_cursor_value = -1;
}

void aeron_cluster_publication_group_close(
    aeron_cluster_publication_group_t *group)
{
    if (NULL == group)
    {
        return;
    }

    if (NULL != group->current && NULL != group->close_func)
    {
        group->close_func(group->current);
    }
}

void aeron_cluster_publication_group_shuffle(
    aeron_cluster_publication_group_t *group)
{
    if (NULL == group)
    {
        return;
    }

    aeron_cluster_publication_group_close(group);
    aeron_cluster_publication_group_clear_exclusion(group);

    for (int i = group->endpoints_length - 1; i > 0; i--)
    {
        const int j = rand() % (i + 1);
        char *tmp = group->endpoints[i];
        group->endpoints[i] = group->endpoints[j];
        group->endpoints[j] = tmp;
    }
}
