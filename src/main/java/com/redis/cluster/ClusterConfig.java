package com.redis.cluster;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * ClusterConfig — Phase 5's first file. Pure logic, no networking: given a
 * key, answer "which node in the cluster owns this key?" Deliberately kept
 * network-free so it's trivially unit-testable, exactly like RESPParser was
 * in Phase 3 — the actual networking wiring (redirecting a client to the
 * right node) comes in a later file, once this logic is proven correct on
 * its own.
 *
 * ===== The core idea: HASH SLOTS =====
 * We need a way to split all possible keys across 3 nodes. The simplest
 * idea — "node = hash(key) % 3" — has a serious problem: if you ever
 * change the number of nodes (add a 4th, remove one), the formula changes
 * for EVERY key, meaning almost every key would suddenly "belong" to a
 * different node than before, forcing a massive reshuffle of nearly all
 * your data just to add one machine.
 *
 * Real Redis Cluster's fix, which we copy here, is to add a layer of
 * indirection: hash every key into one of a large FIXED number of "slots"
 * (Redis uses 16384, and so do we — matching the real system is a nice
 * detail for a portfolio project, and the number itself is arbitrary, just
 * fixed forever). Slots themselves NEVER move or change count. What CAN
 * change is which NODE owns which RANGE of slots — e.g. moving slot range
 * 5000-5460 from node1 to a new node4 only requires migrating the (small)
 * subset of keys that hash into exactly those slots, not touching anything
 * else. We don't build slot MIGRATION in this phase (that's a natural
 * Phase 5-extension or Phase 10 topic) — but structuring things around
 * slots from day one is what makes migration possible later without a
 * rewrite.
 *
 * ===== Math.floorMod — a new method, small but worth calling out =====
 * String.hashCode() can return a NEGATIVE int (it's a 32-bit hash over the
 * full int range, not guaranteed non-negative). Java's plain `%` operator
 * keeps the SIGN of its left operand, so `-7 % 3` evaluates to `-1`, not
 * `2` — completely useless as an array/slot index, which must be
 * non-negative. `Math.floorMod(a, b)` instead always returns a result with
 * the SAME SIGN AS THE DIVISOR (so, non-negative here, since our divisor
 * TOTAL_SLOTS is positive) — exactly the "always give me a valid,
 * non-negative index" behavior we want when turning a hash into a slot
 * number.
 *
 * ===== `record` — a new Java feature, first appearance in this project =====
 * Every other data-holding class in this project so far (RedisValue,
 * CommandResult) has been written the "long way": private final fields, a
 * constructor, and manual getters. A `record` is Java's built-in shorthand
 * for EXACTLY that pattern, when all you need is "an immutable bundle of a
 * few named values." Writing `record NodeInfo(String id, String host, int
 * port, int slotStart, int slotEnd) {}` automatically generates: private
 * final fields for each component, a constructor taking all of them in
 * order, and a getter for each with the SAME NAME as the field (so `id()`,
 * `host()`, `port()`, etc. — not `getId()`; this is a deliberate, different
 * naming convention from regular Java getters, specific to records). It
 * also generates working equals()/hashCode()/toString() for free. We reach
 * for a record here specifically because NodeInfo has no special
 * construction rules to protect (unlike RedisValue, which uses private
 * constructor + factory methods to prevent invalid type+data combinations)
 * — it's genuinely just "5 plain values traveling together," which is
 * exactly what records are for.
 */
public class ClusterConfig {

    // The fixed slot-space size. Chosen to match real Redis Cluster, but
    // the exact number is otherwise arbitrary — what matters is that it
    // never changes once a cluster is running, since every node must agree
    // on it to compute slots consistently.
    public static final int TOTAL_SLOTS = 16384;

    /**
     * One entry in the cluster's topology: a single node's identity,
     * network address, and the (inclusive) range of slots it owns.
     * `ownsSlot` is a small helper method living right on the record — you
     * CAN add extra methods to a record body, beyond the auto-generated
     * ones, whenever a method's logic clearly and only concerns that
     * record's own data.
     */
    public record NodeInfo(String id, String host, int port, int slotStart, int slotEnd) {
        public boolean ownsSlot(int slot) {
            return slot >= slotStart && slot <= slotEnd;
        }
    }

    // The full topology: every node in the cluster, including this one.
    private final List<NodeInfo> allNodes;

    // Which entry in allNodes corresponds to THIS running server process.
    // Every node loads the exact same cluster.nodes list from its config,
    // but each node's config also says cluster.self=<its own id>, so each
    // node can tell "which one of these am I?"
    private final NodeInfo self;

    // Private constructor + a static factory below (fromProperties) —
    // same "static factory method" pattern RedisValue used in Phase 1:
    // it lets us do validation and parsing work BEFORE the object exists,
    // rather than leaving a ClusterConfig half-constructed if the config
    // turns out to be invalid partway through building it.
    private ClusterConfig(List<NodeInfo> allNodes, NodeInfo self) {
        this.allNodes = allNodes;
        this.self = self;
    }

    /**
     * Parses cluster topology out of a Properties object — the same
     * Properties Main.java already loads from each node's config file
     * (Phase 3). Two new properties this method expects to find there:
     *
     *   cluster.self=node1
     *   cluster.nodes=node1:127.0.0.1:6379:0-5460,node2:127.0.0.1:6380:5461-10922,node3:127.0.0.1:6381:10923-16383
     *
     * Every node's config file lists the SAME cluster.nodes value (the
     * whole cluster's layout is identical everywhere) but a DIFFERENT
     * cluster.self value (each node's own id). This is a "static" cluster
     * definition — decided up front and never changing while the cluster
     * runs — which is why one shared properties string is enough; a
     * cluster that could add/remove nodes at runtime would need something
     * more dynamic (like the gossip protocols real Redis Cluster uses),
     * which is out of scope here.
     */
    public static ClusterConfig fromProperties(Properties properties) {
        String selfId = properties.getProperty("cluster.self");
        String nodesRaw = properties.getProperty("cluster.nodes");
        if (selfId == null || nodesRaw == null) {
            throw new IllegalArgumentException(
                    "config file must set both cluster.self and cluster.nodes");
        }

        List<NodeInfo> parsedNodes = new ArrayList<>();
        NodeInfo selfNode = null;

        // cluster.nodes is a comma-separated list; each entry looks like
        // "node1:127.0.0.1:6379:0-5460" — split on ":" gives us
        // [id, host, port, "start-end"], then we split that last piece on
        // "-" to get the two slot numbers.
        for (String entry : nodesRaw.split(",")) {
            String[] parts = entry.trim().split(":");
            String id = parts[0];
            String host = parts[1];
            int port = Integer.parseInt(parts[2]);
            String[] slotRange = parts[3].split("-");
            int slotStart = Integer.parseInt(slotRange[0]);
            int slotEnd = Integer.parseInt(slotRange[1]);

            NodeInfo node = new NodeInfo(id, host, port, slotStart, slotEnd);
            parsedNodes.add(node);
            if (id.equals(selfId)) {
                selfNode = node;
            }
        }

        if (selfNode == null) {
            throw new IllegalArgumentException(
                    "cluster.self=" + selfId + " was not found among the entries in cluster.nodes");
        }

        return new ClusterConfig(parsedNodes, selfNode);
    }

    /**
     * Hashes a key down to a slot number in [0, TOTAL_SLOTS). We use
     * Java's built-in String.hashCode() rather than reimplementing a
     * cryptographic or CRC hash function ourselves — we don't need
     * cryptographic properties here, just a reasonably even spread of
     * keys across slots, and hashCode() does that well enough for a
     * learning project at realistic scale.
     *
     * (Real Redis Cluster instead uses a specific CRC16 algorithm for
     * this step. We deliberately don't replicate that here: nothing about
     * our own cluster's correctness depends on matching Redis's exact
     * hash function, since WE are the ones both computing slots AND
     * telling clients where to go — a real redis-cli in cluster mode
     * doesn't need to predict our slot numbers itself, it just follows
     * whatever MOVED reply we send it, as you'll see in a later file.)
     */
    public int computeSlot(String key) {
        return Math.floorMod(key.hashCode(), TOTAL_SLOTS);
    }

    /**
     * Given a slot number, returns whichever NodeInfo's range covers it.
     * Throws if no configured node covers that slot at all — which would
     * mean the cluster.nodes config has a gap in its ranges, a
     * configuration bug we want to fail loudly on rather than silently
     * misroute keys for.
     */
    public NodeInfo ownerOf(int slot) {
        for (NodeInfo node : allNodes) {
            if (node.ownsSlot(slot)) {
                return node;
            }
        }
        throw new IllegalStateException(
                "no configured node owns slot " + slot
                        + " - check that cluster.nodes covers all " + TOTAL_SLOTS + " slots with no gaps");
    }

    /** Convenience: hash the key AND look up its owner in one call. */
    public NodeInfo ownerOfKey(String key) {
        return ownerOf(computeSlot(key));
    }

    /** Which NodeInfo, out of allNodes, represents this running server. */
    public NodeInfo self() {
        return self;
    }

    /** True if THIS node is the one that owns the given key. */
    public boolean isOwnedByMe(String key) {
        return ownerOfKey(key).id().equals(self.id());
    }
}
