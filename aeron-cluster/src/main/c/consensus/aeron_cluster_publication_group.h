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

#ifndef AERON_CLUSTER_PUBLICATION_GROUP_H
#define AERON_CLUSTER_PUBLICATION_GROUP_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C"
{
#endif

/**
 * Factory function type for creating an exclusive publication.
 * Mirrors Java PublicationGroup.PublicationFactory.
 *
 * @param clientd   user-supplied context (e.g. pointer to aeron_t).
 * @param channel   fully-resolved channel string with endpoint inserted.
 * @param stream_id the stream id for the publication.
 * @return opaque publication pointer, or NULL on failure.
 */
typedef void *(*aeron_cluster_publication_group_factory_func_t)(
    void *clientd, const char *channel, int32_t stream_id);

/**
 * Close function type for closing a publication.
 */
typedef void (*aeron_cluster_publication_group_close_func_t)(void *publication);

/**
 * Round-robin wrapper around a set of endpoints that lazily creates exclusive
 * publications.  Mirrors Java io.aeron.cluster.PublicationGroup<ExclusivePublication>.
 */
typedef struct aeron_cluster_publication_group_stct
{
    char **endpoints;
    int endpoints_length;
    char *channel_template;
    int32_t stream_id;
    aeron_cluster_publication_group_factory_func_t factory;
    aeron_cluster_publication_group_close_func_t close_func;
    int cursor;
    int excluded_cursor_value;
    void *current;
}
aeron_cluster_publication_group_t;

/**
 * Initialise a publication group.
 *
 * @param group             the group struct to initialise (caller-allocated).
 * @param endpoints         array of endpoint strings (copied).
 * @param endpoints_length  number of endpoints.
 * @param channel_template  Aeron URI template, e.g. "aeron:udp?term-length=64k".
 * @param stream_id         stream id for publications.
 * @param factory           function to create a publication.
 * @param close_func        function to close a publication (may be NULL).
 * @return 0 on success, -1 on error.
 */
int aeron_cluster_publication_group_init(
    aeron_cluster_publication_group_t *group,
    const char *const *endpoints,
    int endpoints_length,
    const char *channel_template,
    int32_t stream_id,
    aeron_cluster_publication_group_factory_func_t factory,
    aeron_cluster_publication_group_close_func_t close_func);

/**
 * Free resources held by the group (does NOT close the current publication).
 * Call aeron_cluster_publication_group_close first if you want to close
 * the current publication.
 */
void aeron_cluster_publication_group_fini(aeron_cluster_publication_group_t *group);

/**
 * Advance to the next endpoint (round-robin, skipping the excluded index),
 * close the current publication if any, and create a new one.
 *
 * @param group   the publication group.
 * @param clientd passed through to the factory function.
 * @return the new publication pointer, or NULL on error.
 */
void *aeron_cluster_publication_group_next(
    aeron_cluster_publication_group_t *group, void *clientd);

/**
 * Return the current publication (may be NULL if next has not been called).
 */
void *aeron_cluster_publication_group_current(
    const aeron_cluster_publication_group_t *group);

/**
 * Close the current publication and mark its endpoint as excluded.
 */
void aeron_cluster_publication_group_close_and_exclude_current(
    aeron_cluster_publication_group_t *group);

/**
 * Clear any endpoint exclusion so all endpoints participate in round-robin.
 */
void aeron_cluster_publication_group_clear_exclusion(
    aeron_cluster_publication_group_t *group);

/**
 * Close the current publication (if any).
 */
void aeron_cluster_publication_group_close(
    aeron_cluster_publication_group_t *group);

/**
 * Shuffle the endpoint order randomly, closing the current publication and
 * clearing any exclusion.
 */
void aeron_cluster_publication_group_shuffle(
    aeron_cluster_publication_group_t *group);

#ifdef __cplusplus
}
#endif

#endif /* AERON_CLUSTER_PUBLICATION_GROUP_H */
