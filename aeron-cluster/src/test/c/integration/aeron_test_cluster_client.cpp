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
 * Client helpers for TestCluster (Phase 2b). Lives in its own TU because the
 * client-side `aeron_cluster_t` typedef clashes with the service-side one, so
 * TestCluster.cpp (which uses service headers) can't include
 * client/aeron_cluster.h directly. We expose plain C functions taking void*
 * for the client handle.
 */

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>

extern "C"
{
#include "aeronc.h"
#include "client/aeron_cluster.h"
#include "client/aeron_cluster_context.h"
#include "client/aeron_cluster_client.h"
#include "client/aeron_cluster_async_connect.h"
}

/* Egress callback — increments the response counter supplied by TestCluster. */
static void test_cluster_on_egress(
    void *clientd,
    int64_t /*cluster_session_id*/, int64_t /*leadership_term_id*/, int64_t /*timestamp*/,
    const uint8_t * /*buffer*/, size_t /*length*/, aeron_header_t * /*header*/)
{
    auto *counter = static_cast<std::atomic<int> *>(clientd);
    if (counter) { counter->fetch_add(1); }
}

extern "C"
int aeron_test_cluster_client_start(
    void **out_opaque_async,
    aeron_cluster_context_t **out_client_ctx,
    const char *aeron_dir,
    const char *ingress_endpoints,
    int egress_port,
    std::atomic<int> *response_counter)
{
    if (!out_opaque_async || !out_client_ctx) { return -1; }

    aeron_cluster_context_t *ctx = nullptr;
    if (aeron_cluster_context_init(&ctx) < 0) { return -1; }

    aeron_cluster_context_set_aeron_directory_name(ctx, aeron_dir);

    /* The C cluster client uses `ingress_channel` as a concrete channel URI
     * for its initial ingress publication and then reacts to REDIRECT events
     * from the leader to update `ingress_endpoints`. We point the initial
     * channel at the first endpoint in the endpoints string.
     *
     * ingress_endpoints format: "id1=host:port,id2=host:port,..." */
    char first_endpoint[128] = {0};
    const char *eq = strchr(ingress_endpoints, '=');
    const char *comma = strchr(ingress_endpoints, ',');
    size_t ep_len = 0;
    if (eq != nullptr)
    {
        const char *ep_start = eq + 1;
        const char *ep_end   = (comma != nullptr && comma > ep_start) ? comma
                                                                      : ingress_endpoints +
                                                                            strlen(ingress_endpoints);
        ep_len = static_cast<size_t>(ep_end - ep_start);
        if (ep_len >= sizeof(first_endpoint)) { ep_len = sizeof(first_endpoint) - 1; }
        memcpy(first_endpoint, ep_start, ep_len);
        first_endpoint[ep_len] = '\0';
    }

    char ingress_buf[160];
    snprintf(ingress_buf, sizeof(ingress_buf),
             "aeron:udp?endpoint=%s", first_endpoint);
    aeron_cluster_context_set_ingress_channel(ctx, ingress_buf);
    aeron_cluster_context_set_ingress_stream_id(ctx, 101);

    char egress_buf[128];
    snprintf(egress_buf, sizeof(egress_buf), "aeron:udp?endpoint=localhost:%d", egress_port);
    aeron_cluster_context_set_egress_channel(ctx, egress_buf);
    aeron_cluster_context_set_egress_stream_id(ctx, 102);

    aeron_cluster_context_set_ingress_endpoints(ctx, ingress_endpoints);
    aeron_cluster_context_set_on_message(ctx, test_cluster_on_egress, response_counter);
    /* message_timeout_ns is the async_connect deadline measured by real clock;
     * tests tick slowly (20 ms virtual / 10 ms real) so we need plenty of
     * headroom. 60 s is what echo_test uses for the same reason. */
    aeron_cluster_context_set_message_timeout_ns(ctx, INT64_C(60000000000));

    aeron_cluster_async_connect_t *async = nullptr;
    if (aeron_cluster_async_connect(&async, ctx) < 0)
    {
        aeron_cluster_context_close(ctx);
        return -1;
    }

    *out_opaque_async = async;
    *out_client_ctx   = ctx;
    return 0;
}

extern "C"
int aeron_test_cluster_client_poll_connect(
    void **io_opaque_async, void **out_opaque_client)
{
    auto *async = static_cast<aeron_cluster_async_connect_t *>(*io_opaque_async);
    aeron_cluster_t *client = nullptr;
    const int rc = aeron_cluster_async_connect_poll(&client, async);
    if (rc > 0 && client != nullptr)
    {
        *out_opaque_client = client;
        /* async is consumed on success */
        *io_opaque_async = nullptr;
    }
    else if (rc < 0)
    {
        /* async_connect_poll frees the async handle internally on error */
        *io_opaque_async = nullptr;
    }
    return rc;
}

extern "C"
int64_t aeron_test_cluster_client_offer(
    void *opaque_client, const void *buffer, size_t length)
{
    auto *client = static_cast<aeron_cluster_t *>(opaque_client);
    if (!client) { return -1; }
    return aeron_cluster_offer(
        client, static_cast<const uint8_t *>(buffer), length);
}

extern "C"
void aeron_test_cluster_client_poll_egress(void *opaque_client)
{
    auto *client = static_cast<aeron_cluster_t *>(opaque_client);
    if (client) { aeron_cluster_poll_egress(client); }
}

extern "C"
void aeron_test_cluster_client_close(
    void *opaque_client, void *opaque_async, aeron_cluster_context_t *client_ctx)
{
    if (opaque_client)
    {
        aeron_cluster_close(static_cast<aeron_cluster_t *>(opaque_client));
    }
    else if (opaque_async)
    {
        aeron_cluster_async_connect_delete(
            static_cast<aeron_cluster_async_connect_t *>(opaque_async));
    }
    if (client_ctx)
    {
        /* If the async connect's internal ctx took ownership of the aeron
         * client, null it out here to avoid double-free. */
        client_ctx->aeron = nullptr;
        client_ctx->owns_aeron_client = false;
        aeron_cluster_context_close(client_ctx);
    }
}
