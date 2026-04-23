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

#ifndef AERON_TEST_CLUSTERED_MEDIA_DRIVER_H
#define AERON_TEST_CLUSTERED_MEDIA_DRIVER_H

#include <string>
#include <iostream>

/**
 * C equivalent of Java ClusteredMediaDriver: bundles MediaDriver + Archive + ConsensusModule.
 *
 * Identity model (aligned with Java TestCluster):
 *   - member_id ∈ [0, member_count) — member identity; iterated 0..n-1 identically
 *     across nodes so every node computes the same cluster_members /
 *     ingress_endpoints view
 *   - port_base — starting port offset to disambiguate concurrent tests
 *     (plays Java `clusterId`'s role). All nodes in a cluster share the
 *     same port_base; ports are derived from (port_base + member_id)
 *
 * Old callers supplied a single `node_index` that encoded both roles
 * (node_index = port_base + member_id). Each node re-derived port_base via
 * `base = node_index - (node_index % member_count)`, which diverged across
 * nodes when port_base wasn't a multiple of member_count. Splitting the
 * args fixes that and matches Java's (clusterId, memberId, memberCount).
 */
class TestClusteredMediaDriver
{
public:
    TestClusteredMediaDriver(
        int member_id,
        int member_count,
        int port_base,
        const std::string &base_dir,
        std::ostream &stream);

    ~TestClusteredMediaDriver();

    /** Launch only MediaDriver + Archive (no CM). Returns 0 on success. */
    int launch_driver_only();

    /** Launch all three: MediaDriver + Archive + ConsensusModule. Returns 0 on success. */
    int launch();

    /** Close all components (CM thread, CM agent, archiving driver). */
    void close();

    /** Returns true if CM agent is leader. */
    bool is_leader() const;

    const std::string &aeron_dir() const;
    const std::string &archive_dir() const;
    const std::string &cluster_dir() const;
    const std::string &cluster_members() const;
    const std::string &ingress_endpoints() const;

    /** Legacy accessor returning port_base + member_id — kept for callers
     * that used it as a node identifier in logs. */
    int node_index() const;

private:
    struct Impl;
    Impl *m_impl;
};

#endif /* AERON_TEST_CLUSTERED_MEDIA_DRIVER_H */
