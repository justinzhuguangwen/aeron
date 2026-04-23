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

#ifndef AERON_TEST_CLUSTER_H
#define AERON_TEST_CLUSTER_H

#include <atomic>
#include <cstdint>
#include <string>
#include <thread>
#include <vector>

#include "aeron_test_cluster_node.h"

extern "C"
{
#include "aeron_cm_context.h"
#include "aeron_consensus_module_agent.h"
#include "aeron_cluster_service_context.h"
#include "aeron_clustered_service_agent.h"
#include "aeron_cluster_service.h"
}

/* Forward declare client-side aeron_cluster_context_t only.
 * The full client header cannot be included here because it typedefs
 * `aeron_cluster_t` to a different struct than the service header does, and
 * this header is compiled by both worlds. */
struct aeron_cluster_context_stct;
typedef struct aeron_cluster_context_stct aeron_cluster_context_t;

/**
 * Multi-node cluster test harness — C equivalent of Java TestCluster
 * (aeron-test-support/.../TestCluster.java).
 *
 * Phase 1: election + failover on CM-only (virtual-time tick loop).
 * Phase 2a attempt: drive per-node service from the same virtual-time tick —
 *   ×57 slowdown, reverted.
 * Phase 2b: per-node service on its own wall-clock background thread, while
 *   CM stays on the virtual-time tick. Enabled only when `start_all()` is
 *   called with `with_service = true`.  Services that a test does not need
 *   stay disabled so existing Phase 1 tests keep their fast virtual-time
 *   behaviour.
 *
 * Phase 3 (snapshot) remains TODO.
 *
 * Timing aligned with Java TestCluster constants:
 *   - startup canvass 2 s, election 500 ms, heartbeat 1 s
 */
class TestCluster
{
public:
    /**
     * Construct a cluster with node_count members numbered
     * base_node_index .. base_node_index + node_count - 1.
     * Does not start anything — call start_all() next.
     */
    TestCluster(int node_count, int base_node_index, const std::string &test_name);
    ~TestCluster();

    TestCluster(const TestCluster &) = delete;
    TestCluster &operator=(const TestCluster &) = delete;

    /**
     * Start all nodes' MediaDriver+Archive, then create all CM agents with
     * the given appointed_leader_id (-1 for organic election).
     * If with_service is true, each node also runs an echo service on its own
     * wall-clock background thread (Phase 2b). Services default off because
     * adding them to the virtual-time tick is unworkable (Phase 2a, reverted).
     * Returns 0 on success, -1 on failure with aeron_errmsg() set.
     */
    int start_all(int appointed_leader_id = -1, bool with_service = false);

    /**
     * Drive do_work on every live agent until a leader emerges, advancing
     * virtual time by 20 ms per tick. Returns the leader index, or -1 if no
     * leader was observed within max_ticks.
     * Also caches leader_index() and records m_now_ns.
     *
     * A "leader" means both role == LEADER and election == NULL (i.e. the
     * election has closed). This avoids a race where the caller kills the
     * leader the moment role flips to LEADER: the enter-election path on the
     * freshly-demoted leader is gated on election == NULL, so without waiting
     * for that gate to re-open, the quorum-loss detection path never fires.
     *
     * Use await_leader_role() when only the role transition matters.
     */
    int await_leader(int max_ticks = 2000);

    /**
     * Like await_leader but only waits for role == LEADER on any node — does
     * not require that node's election struct to have fully closed. Useful
     * when a transient election is expected (e.g. after restarting a follower
     * that triggers a re-evaluation).
     */
    int await_leader_role(int max_ticks = 2000);

    /** Cached leader index from the most recent await_leader; -1 if unknown. */
    int leader_index() const { return m_leader_idx; }

    /** True iff node idx is alive and currently in LEADER role. */
    bool is_leader(int idx) const;

    /** True iff node idx is alive and in FOLLOWER role. */
    bool is_follower(int idx) const;

    /**
     * Fully stop node idx: close CM agent, close CM context, stop media driver,
     * delete node. After this call, node(idx) == nullptr and cm_agent(idx) == nullptr.
     * No-op if the node was already stopped.
     */
    void stop_node(int idx);

    /**
     * Convenience: stop the current leader (as cached by await_leader/is_leader).
     * Returns the stopped leader's index, or -1 if no leader is known.
     */
    int stop_leader();

    /**
     * Restart a previously stopped node — re-creates driver+archive and CM agent.
     * Uses the same base_dir and node identity. Returns 0 on success, -1 on failure.
     * Appointed-leader mode uses the initial value passed to start_all().
     */
    int restart_node(int idx);

    /**
     * Drive do_work on every live agent for ticks iterations, advancing virtual
     * time by 20 ms per tick and sleeping 10 ms of wall time to let the media
     * driver threads progress UDP delivery.
     * Does not look for a leader — use await_leader/await_role for state checks.
     */
    void drive(int ticks);

    /**
     * Drive until the predicate returns true, or max_ticks elapses.
     * Returns true if the predicate became true.
     */
    template <typename Pred>
    bool await(Pred &&pred, int max_ticks = 2000)
    {
        for (int t = 0; t < max_ticks; ++t)
        {
            tick_();
            if (pred()) { return true; }
        }
        return pred();
    }

    /* ---------------- Phase 2b: client API ----------------
     * These drive a single cluster client that connects via the ingress
     * endpoints constructed from the node base index. The client's egress
     * callback bumps an internal counter so await_response_count can wait for
     * the expected number of echoes.
     *
     * Client methods use the service background thread for server-side work,
     * and the caller's thread for tick_() to drive the CMs and
     * async_connect_poll / poll_egress.
     *
     * Preconditions: start_all(..., with_service=true) succeeded and
     * await_leader returned a valid index. */
    int  connect_client(int64_t timeout_ns = INT64_C(30000000000));
    int64_t offer(const void *buffer, size_t length);
    void poll_egress_once();
    int  await_response_count(int expected, int max_wall_ms = 10000);
    int  response_count() const;

    /**
     * Most recent role the clustered service on node idx observed via its
     * on_role_change callback. Returns -1 if the service hasn't been created
     * or hasn't had a role yet. Enum values match aeron_cluster_role_t:
     *   0=FOLLOWER, 1=CANDIDATE, 2=LEADER.
     */
    int service_last_role(int idx) const;

    /* Accessors */
    aeron_consensus_module_agent_t *cm_agent(int idx) const
    {
        return (idx >= 0 && static_cast<size_t>(idx) < m_agents.size())
            ? m_agents[idx] : nullptr;
    }

    aeron_clustered_service_agent_t *svc_agent(int idx) const
    {
        return (idx >= 0 && static_cast<size_t>(idx) < m_svc_agents.size())
            ? m_svc_agents[idx] : nullptr;
    }

    TestClusterNode *node(int idx) const
    {
        return (idx >= 0 && static_cast<size_t>(idx) < m_nodes.size())
            ? m_nodes[idx] : nullptr;
    }

    int node_count() const { return m_node_count; }
    const std::string &base_dir() const { return m_base_dir; }

private:
    /* Create MediaDriver+Archive for one node (assumes slot empty). */
    int start_driver_(int idx);

    /* Create CM context + CM agent for one node (assumes driver running). */
    int create_agent_(int idx);

    /* Create service context + service agent + background driving thread. */
    int create_service_(int idx);

    /* Close CM agent + context for one node (driver untouched). */
    void close_agent_(int idx);

    /* Stop service thread, close service agent + context. */
    void close_service_(int idx);

    /* Advance one tick: +20 ms virtual, do_work each alive agent, 10 ms sleep. */
    void tick_();

    int                                              m_node_count;
    int                                              m_base_node_index;
    int                                              m_appointed_leader_id = -1;
    bool                                             m_with_service        = false;
    std::string                                      m_base_dir;
    std::vector<TestClusterNode *>                   m_nodes;
    std::vector<aeron_cm_context_t *>                m_cm_ctx;
    std::vector<aeron_consensus_module_agent_t *>    m_agents;
    std::vector<aeron_cluster_service_context_t *>   m_svc_ctx;
    std::vector<aeron_clustered_service_agent_t *>   m_svc_agents;
    std::vector<aeron_clustered_service_t *>         m_svc_impls;
    std::vector<void *>                              m_svc_clientd;   /* test_service_clientd* (opaque) */
    std::vector<std::thread>                         m_svc_threads;
    std::vector<std::atomic<bool> *>                 m_svc_running;
    int64_t                                          m_now_ns     = 0;
    int                                              m_leader_idx = -1;

    /* Phase 2b client state — stored as void* so the header stays free of
     * the client-side aeron_cluster_t typedef. */
    aeron_cluster_context_t                         *m_client_ctx    = nullptr;
    void                                            *m_client_opaque = nullptr;
    std::atomic<int>                                 m_response_count{0};
};

#endif /* AERON_TEST_CLUSTER_H */
