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

#include "aeron_test_cluster.h"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <thread>

extern "C"
{
#include "aeronc.h"
#include "aeron_archive.h"
#include "util/aeron_fileutil.h"
#include "service/aeron_cluster_client_session.h"
}

/* Client-side helpers live in aeron_test_cluster_client.cpp — that TU includes
 * only the client headers to avoid the aeron_cluster_t typedef clash with the
 * service headers that this TU uses. */
extern "C"
{
int aeron_test_cluster_client_start(
    void **out_opaque_async,
    aeron_cluster_context_t **out_client_ctx,
    const char *aeron_dir,
    const char *ingress_endpoints,
    int egress_port,
    std::atomic<int> *response_counter);
int aeron_test_cluster_client_poll_connect(void **io_opaque_async, void **out_opaque_client);
int64_t aeron_test_cluster_client_offer(void *opaque_client, const void *buffer, size_t length);
void aeron_test_cluster_client_poll_egress(void *opaque_client);
void aeron_test_cluster_client_close(
    void *opaque_client, void *opaque_async, aeron_cluster_context_t *client_ctx);
}

namespace
{
std::string make_base_dir(const std::string &test_name)
{
    char base[AERON_MAX_PATH] = {0};
    aeron_temp_filename(base, sizeof(base));
    return std::string(base) + "-" + test_name;
}

/* Per-node service data passed as `clientd` into the service callbacks. Holds
 * the service agent pointer (for offering egress from the echo callback) plus
 * an atomic that records the most recent role observed by on_role_change. */
struct test_service_clientd
{
    aeron_clustered_service_agent_t *agent;
    std::atomic<int>                 last_role{-1};
};

/* Echo service callbacks — reply to every inbound session message with the
 * same bytes. Mirrors the single-node echo in aeron_cluster_server_helper.cpp. */
void echo_on_start(void *, aeron_cluster_t *, aeron_cluster_snapshot_image_t *) {}
void echo_on_terminate(void *, aeron_cluster_t *) {}
void echo_on_session_message(
    void *clientd, aeron_cluster_client_session_t *session,
    int64_t /*timestamp*/, const uint8_t *buffer, size_t length)
{
    auto *data = static_cast<test_service_clientd *>(clientd);
    if (data && data->agent && session)
    {
        aeron_cluster_client_session_offer(
            session, data->agent->leadership_term_id, buffer, length);
    }
}
void echo_on_role_change(void *clientd, aeron_cluster_role_t new_role)
{
    auto *data = static_cast<test_service_clientd *>(clientd);
    if (data) { data->last_role.store(static_cast<int>(new_role)); }
}
}

TestCluster::TestCluster(int node_count, int base_node_index, const std::string &test_name)
    : m_node_count(node_count),
      m_base_node_index(base_node_index),
      m_base_dir(make_base_dir(test_name)),
      m_nodes(node_count, nullptr),
      m_cm_ctx(node_count, nullptr),
      m_agents(node_count, nullptr),
      m_svc_ctx(node_count, nullptr),
      m_svc_agents(node_count, nullptr),
      m_svc_impls(node_count, nullptr),
      m_svc_clientd(node_count, nullptr),
      m_svc_threads(node_count),
      m_svc_running(node_count, nullptr)
{
    aeron_delete_directory(m_base_dir.c_str());
}

TestCluster::~TestCluster()
{
    if (m_client_opaque != nullptr || m_client_ctx != nullptr)
    {
        aeron_test_cluster_client_close(m_client_opaque, nullptr, m_client_ctx);
        m_client_opaque = nullptr;
        m_client_ctx    = nullptr;
    }
    for (int i = 0; i < m_node_count; ++i)
    {
        stop_node(i);
    }
    aeron_delete_directory(m_base_dir.c_str());
}

int TestCluster::start_all(int appointed_leader_id, bool with_service)
{
    m_appointed_leader_id = appointed_leader_id;
    m_with_service        = with_service;

    for (int i = 0; i < m_node_count; ++i)
    {
        if (start_driver_(i) < 0) { return -1; }
    }
    /* Services must be created + their background thread running BEFORE the CM
     * starts so the service's control subscription is ready when the CM sends
     * its first JoinLog. */
    if (with_service)
    {
        for (int i = 0; i < m_node_count; ++i)
        {
            if (create_service_(i) < 0) { return -1; }
        }
    }
    for (int i = 0; i < m_node_count; ++i)
    {
        if (create_agent_(i) < 0) { return -1; }
    }
    return 0;
}

int TestCluster::start_driver_(int idx)
{
    if (m_nodes[idx] != nullptr) { return 0; }

    m_nodes[idx] = new TestClusterNode(
        idx, m_node_count, m_base_node_index, m_base_dir, std::cout);
    m_nodes[idx]->start();
    return 0;
}

int TestCluster::create_agent_(int idx)
{
    if (m_nodes[idx] == nullptr) { return -1; }

    aeron_cm_context_t *ctx = nullptr;
    if (aeron_cm_context_init(&ctx) < 0) { return -1; }

    snprintf(ctx->aeron_directory_name, sizeof(ctx->aeron_directory_name),
             "%s", m_nodes[idx]->aeron_dir().c_str());
    ctx->member_id            = idx;
    ctx->appointed_leader_id  = m_appointed_leader_id;
    ctx->service_count        = m_with_service ? 1 : 0;
    ctx->app_version          = 1;

    if (ctx->cluster_members) { free(ctx->cluster_members); }
    ctx->cluster_members = strdup(m_nodes[idx]->cluster_members().c_str());
    strncpy(ctx->cluster_dir, m_nodes[idx]->cluster_dir().c_str(),
            sizeof(ctx->cluster_dir) - 1);

    if (ctx->consensus_channel) { free(ctx->consensus_channel); }
    ctx->consensus_channel        = strdup("aeron:udp");
    ctx->consensus_stream_id      = 108;
    if (ctx->log_channel) { free(ctx->log_channel); }
    ctx->log_channel              = strdup("aeron:ipc");
    ctx->log_stream_id            = 100;
    if (ctx->snapshot_channel) { free(ctx->snapshot_channel); }
    ctx->snapshot_channel         = strdup("aeron:ipc");
    ctx->snapshot_stream_id       = 107;
    if (ctx->control_channel) { free(ctx->control_channel); }
    ctx->control_channel          = strdup("aeron:ipc");
    ctx->consensus_module_stream_id = 105;
    ctx->service_stream_id          = 104;
    if (ctx->ingress_channel) { free(ctx->ingress_channel); }
    ctx->ingress_channel          = strdup("aeron:udp");
    ctx->ingress_stream_id        = 101;

    /* Java TestCluster timeout constants */
    ctx->startup_canvass_timeout_ns    = INT64_C(2000000000);
    ctx->election_timeout_ns           = INT64_C(500000000);
    ctx->election_status_interval_ns   = INT64_C(100000000);
    ctx->leader_heartbeat_timeout_ns   = INT64_C(1000000000);
    ctx->leader_heartbeat_interval_ns  = INT64_C(100000000);
    ctx->session_timeout_ns            = INT64_C(10000000000);
    ctx->termination_timeout_ns        = INT64_C(1000000000);

    aeron_archive_context_t *arch_ctx = nullptr;
    aeron_archive_context_init(&arch_ctx);
    aeron_archive_context_set_aeron_directory_name(arch_ctx,
                                                   m_nodes[idx]->aeron_dir().c_str());
    aeron_archive_context_set_control_request_channel(arch_ctx, "aeron:ipc");
    aeron_archive_context_set_control_response_channel(arch_ctx, "aeron:ipc");
    ctx->archive_ctx      = arch_ctx;
    ctx->owns_archive_ctx = true;

    m_cm_ctx[idx] = ctx;

    if (aeron_consensus_module_agent_create(&m_agents[idx], ctx) < 0)
    {
        return -1;
    }
    return aeron_consensus_module_agent_on_start(m_agents[idx]);
}

int TestCluster::create_service_(int idx)
{
    if (m_nodes[idx] == nullptr) { return -1; }

    aeron_cluster_service_context_t *svc_ctx = nullptr;
    if (aeron_cluster_service_context_init(&svc_ctx) < 0) { return -1; }

    aeron_cluster_service_context_set_aeron_directory_name(
        svc_ctx, m_nodes[idx]->aeron_dir().c_str());
    aeron_cluster_service_context_set_service_id(svc_ctx, 0);
    aeron_cluster_service_context_set_control_channel(svc_ctx, "aeron:ipc");
    aeron_cluster_service_context_set_consensus_module_stream_id(svc_ctx, 104);
    aeron_cluster_service_context_set_service_channel(svc_ctx, "aeron:ipc");
    svc_ctx->service_stream_id   = 105;
    aeron_cluster_service_context_set_snapshot_channel(svc_ctx, "aeron:ipc");
    svc_ctx->snapshot_stream_id  = 107;
    svc_ctx->cluster_id          = 0;
    strncpy(svc_ctx->cluster_dir, m_nodes[idx]->cluster_dir().c_str(),
            sizeof(svc_ctx->cluster_dir) - 1);

    auto *impl = new aeron_clustered_service_t{};
    impl->on_start           = echo_on_start;
    impl->on_terminate       = echo_on_terminate;
    impl->on_session_message = echo_on_session_message;
    impl->on_role_change     = echo_on_role_change;
    impl->clientd            = nullptr;  /* updated after agent exists */
    svc_ctx->service         = impl;

    m_svc_ctx[idx]   = svc_ctx;
    m_svc_impls[idx] = impl;

    if (aeron_clustered_service_agent_create(&m_svc_agents[idx], svc_ctx) < 0)
    {
        return -1;
    }

    auto *data = new test_service_clientd();
    data->agent = m_svc_agents[idx];
    m_svc_clientd[idx] = data;
    impl->clientd = data;

    if (aeron_clustered_service_agent_on_start(m_svc_agents[idx]) < 0)
    {
        return -1;
    }

    /* Launch the background driver thread — real wall clock, 100 µs poll,
     * matches aeron_cluster_server_helper.cpp's cluster_server_start_background.
     * The service MUST run on its own thread so the real MediaDriver has wall
     * time to pump IPC messages between CM and service; driving service from
     * the virtual-time tick loop was ~57× too slow (Phase 2a, reverted). */
    m_svc_running[idx] = new std::atomic<bool>(true);
    std::atomic<bool> *running = m_svc_running[idx];
    aeron_clustered_service_agent_t *svc = m_svc_agents[idx];
    m_svc_threads[idx] = std::thread([running, svc]() {
        while (running->load())
        {
            aeron_clustered_service_agent_do_work(svc, aeron_nano_clock());
            std::this_thread::sleep_for(std::chrono::microseconds(100));
        }
    });

    return 0;
}

void TestCluster::close_service_(int idx)
{
    if (m_svc_running[idx])
    {
        m_svc_running[idx]->store(false);
        if (m_svc_threads[idx].joinable())
        {
            m_svc_threads[idx].join();
        }
        delete m_svc_running[idx];
        m_svc_running[idx] = nullptr;
    }
    if (m_svc_agents[idx])
    {
        aeron_clustered_service_agent_close(m_svc_agents[idx]);
        m_svc_agents[idx] = nullptr;
    }
    if (m_svc_ctx[idx])
    {
        /* service_ctx owns nothing about m_svc_impls — null it out before close. */
        m_svc_ctx[idx]->service = nullptr;
        aeron_cluster_service_context_close(m_svc_ctx[idx]);
        m_svc_ctx[idx] = nullptr;
    }
    if (m_svc_impls[idx])
    {
        delete m_svc_impls[idx];
        m_svc_impls[idx] = nullptr;
    }
    if (m_svc_clientd[idx])
    {
        delete static_cast<test_service_clientd *>(m_svc_clientd[idx]);
        m_svc_clientd[idx] = nullptr;
    }
}

void TestCluster::close_agent_(int idx)
{
    if (m_agents[idx])
    {
        aeron_consensus_module_agent_close(m_agents[idx]);
        m_agents[idx] = nullptr;
    }
    if (m_cm_ctx[idx])
    {
        aeron_cm_context_close(m_cm_ctx[idx]);
        m_cm_ctx[idx] = nullptr;
    }
}

void TestCluster::stop_node(int idx)
{
    if (idx < 0 || idx >= m_node_count) { return; }

    /* Close CM before service so the service's JoinLog subscription is torn
     * down cleanly after the CM's log publication goes away. */
    close_agent_(idx);
    if (m_with_service) { close_service_(idx); }

    if (m_nodes[idx])
    {
        m_nodes[idx]->stop();
        delete m_nodes[idx];
        m_nodes[idx] = nullptr;
    }

    if (m_leader_idx == idx) { m_leader_idx = -1; }

    /* Jump past leader_heartbeat_timeout (1 s) + election_timeout (500 ms) so
     * surviving followers can observe the peer as failed on the next tick. */
    m_now_ns += INT64_C(2000000000);
}

int TestCluster::stop_leader()
{
    int idx = m_leader_idx;
    if (idx < 0)
    {
        /* Fall back to scanning in case leader_index is stale. */
        for (int i = 0; i < m_node_count; ++i)
        {
            if (is_leader(i)) { idx = i; break; }
        }
    }
    if (idx < 0) { return -1; }

    stop_node(idx);
    return idx;
}

int TestCluster::restart_node(int idx)
{
    if (idx < 0 || idx >= m_node_count) { return -1; }
    if (m_nodes[idx] != nullptr) { return -1; }  /* not stopped */

    if (start_driver_(idx) < 0)    { return -1; }
    if (m_with_service)
    {
        if (create_service_(idx) < 0) { return -1; }
    }
    if (create_agent_(idx) < 0)    { return -1; }
    return 0;
}

void TestCluster::tick_()
{
    m_now_ns += INT64_C(20000000);
    for (int i = 0; i < m_node_count; ++i)
    {
        if (m_agents[i])
        {
            aeron_consensus_module_agent_do_work(m_agents[i], m_now_ns);
        }
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
}

void TestCluster::drive(int ticks)
{
    for (int t = 0; t < ticks; ++t) { tick_(); }
}

int TestCluster::await_leader(int max_ticks)
{
    if (m_now_ns == 0) { m_now_ns = aeron_nano_clock(); }

    for (int t = 0; t < max_ticks; ++t)
    {
        tick_();
        for (int i = 0; i < m_node_count; ++i)
        {
            /* Wait for election to close as well — role flips to LEADER during
             * election (LEADER_LOG_REPLICATION state) before the election
             * struct is actually closed. Callers that kill the leader
             * immediately after await_leader return would otherwise race with
             * the lingering election and the quorum-loss path (which is gated
             * on election == NULL) would never fire. */
            if (m_agents[i] &&
                AERON_CLUSTER_ROLE_LEADER == m_agents[i]->role &&
                nullptr == m_agents[i]->election)
            {
                m_leader_idx = i;
                return i;
            }
        }
    }
    m_leader_idx = -1;
    return -1;
}

int TestCluster::await_leader_role(int max_ticks)
{
    if (m_now_ns == 0) { m_now_ns = aeron_nano_clock(); }

    for (int t = 0; t < max_ticks; ++t)
    {
        tick_();
        for (int i = 0; i < m_node_count; ++i)
        {
            if (m_agents[i] && AERON_CLUSTER_ROLE_LEADER == m_agents[i]->role)
            {
                m_leader_idx = i;
                return i;
            }
        }
    }
    m_leader_idx = -1;
    return -1;
}

bool TestCluster::is_leader(int idx) const
{
    return cm_agent(idx) != nullptr &&
           AERON_CLUSTER_ROLE_LEADER == cm_agent(idx)->role;
}

bool TestCluster::is_follower(int idx) const
{
    return cm_agent(idx) != nullptr &&
           AERON_CLUSTER_ROLE_FOLLOWER == cm_agent(idx)->role;
}

int TestCluster::service_last_role(int idx) const
{
    if (idx < 0 || static_cast<size_t>(idx) >= m_svc_clientd.size()) { return -1; }
    auto *data = static_cast<test_service_clientd *>(m_svc_clientd[idx]);
    return (data != nullptr) ? data->last_role.load() : -1;
}

/* ----------------------- Phase 2b: client API ----------------------- */

int TestCluster::connect_client(int64_t timeout_ns)
{
    if (m_client_opaque != nullptr) { return -1; }   /* already connected */
    if (m_leader_idx < 0 || m_nodes[m_leader_idx] == nullptr) { return -1; }

    const int egress_port = 24500 + m_base_node_index;

    void *async = nullptr;
    if (aeron_test_cluster_client_start(
            &async, &m_client_ctx,
            m_nodes[m_leader_idx]->aeron_dir().c_str(),
            m_nodes[m_leader_idx]->ingress_endpoints().c_str(),
            egress_port, &m_response_count) < 0)
    {
        return -1;
    }

    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::nanoseconds(timeout_ns);
    void *connected = nullptr;
    while (connected == nullptr && async != nullptr &&
           std::chrono::steady_clock::now() < deadline)
    {
        tick_();
        const int rc = aeron_test_cluster_client_poll_connect(&async, &connected);
        if (rc < 0)
        {
            /* async was freed internally by poll_connect on error. */
            aeron_test_cluster_client_close(nullptr, nullptr, m_client_ctx);
            m_client_ctx = nullptr;
            return -1;
        }
    }

    if (connected == nullptr)
    {
        /* timed out; async may still be live (poll returned 0 each time). */
        aeron_test_cluster_client_close(nullptr, async, m_client_ctx);
        m_client_ctx = nullptr;
        return -1;
    }

    m_client_opaque = connected;
    return 0;
}

int64_t TestCluster::offer(const void *buffer, size_t length)
{
    return aeron_test_cluster_client_offer(m_client_opaque, buffer, length);
}

void TestCluster::poll_egress_once()
{
    aeron_test_cluster_client_poll_egress(m_client_opaque);
}

int TestCluster::response_count() const
{
    return m_response_count.load();
}

int TestCluster::await_response_count(int expected, int max_wall_ms)
{
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::milliseconds(max_wall_ms);
    while (m_response_count.load() < expected &&
           std::chrono::steady_clock::now() < deadline)
    {
        tick_();
        poll_egress_once();
    }
    return m_response_count.load();
}
