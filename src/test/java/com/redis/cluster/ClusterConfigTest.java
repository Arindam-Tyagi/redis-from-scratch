package com.redis.cluster;

import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for ClusterConfig — checking the parsing logic and the slot math,
 * completely without any real server or network involved (exactly the
 * point of keeping ClusterConfig network-free in the first place).
 */
class ClusterConfigTest {

    // A 3-node topology covering the full slot space with no gaps and no
    // overlaps: 0-5460, 5461-10922, 10923-16383 (16384 slots total,
    // 0-indexed, so the last slot number is 16383, not 16384).
    private Properties threeNodeConfig(String selfId) {
        Properties properties = new Properties();
        properties.setProperty("cluster.self", selfId);
        properties.setProperty("cluster.nodes",
                "node1:127.0.0.1:6379:0-5460,"
                        + "node2:127.0.0.1:6380:5461-10922,"
                        + "node3:127.0.0.1:6381:10923-16383");
        return properties;
    }

    @Test
    void parsesAllThreeNodesAndIdentifiesSelfCorrectly() {
        ClusterConfig config = ClusterConfig.fromProperties(threeNodeConfig("node2"));

        ClusterConfig.NodeInfo self = config.self();
        assertEquals("node2", self.id());
        assertEquals("127.0.0.1", self.host());
        assertEquals(6380, self.port());
        assertEquals(5461, self.slotStart());
        assertEquals(10922, self.slotEnd());
    }

    @Test
    void missingClusterSelfThrowsIllegalArgumentException() {
        Properties properties = threeNodeConfig("node1");
        properties.remove("cluster.self");
        assertThrows(IllegalArgumentException.class, () -> ClusterConfig.fromProperties(properties));
    }

    @Test
    void selfIdNotPresentInNodesListThrowsIllegalArgumentException() {
        // "node9" is never listed in cluster.nodes, so ClusterConfig has no
        // way to know which entry is "this server" — should fail loudly
        // rather than silently picking an arbitrary node.
        assertThrows(IllegalArgumentException.class,
                () -> ClusterConfig.fromProperties(threeNodeConfig("node9")));
    }

    @Test
    void computeSlotAlwaysReturnsANonNegativeSlotInRange() {
        ClusterConfig config = ClusterConfig.fromProperties(threeNodeConfig("node1"));

        // Try a handful of different keys, including ones whose hashCode()
        // is very likely negative, to prove Math.floorMod is doing its job
        // (a plain "%" here could return a negative number for some of
        // these, which would break ownerOf's range lookup).
        String[] keys = {"foo", "bar", "user:12345", "", "a-fairly-long-key-name-here"};
        for (String key : keys) {
            int slot = config.computeSlot(key);
            assertTrue(slot >= 0 && slot < ClusterConfig.TOTAL_SLOTS,
                    "slot for \"" + key + "\" was out of range: " + slot);
        }
    }

    @Test
    void computeSlotIsDeterministicForTheSameKey() {
        ClusterConfig config = ClusterConfig.fromProperties(threeNodeConfig("node1"));
        // The same key must always hash to the same slot - if it didn't,
        // a key could seemingly "move" to a different node on every
        // request, which would break the whole point of sharding.
        int first = config.computeSlot("mykey");
        int second = config.computeSlot("mykey");
        assertEquals(first, second);
    }

    @Test
    void ownerOfReturnsTheNodeWhoseRangeCoversTheSlot() {
        ClusterConfig config = ClusterConfig.fromProperties(threeNodeConfig("node1"));

        assertEquals("node1", config.ownerOf(0).id());
        assertEquals("node1", config.ownerOf(5460).id());
        assertEquals("node2", config.ownerOf(5461).id());
        assertEquals("node2", config.ownerOf(10922).id());
        assertEquals("node3", config.ownerOf(10923).id());
        assertEquals("node3", config.ownerOf(16383).id());
    }

    @Test
    void ownerOfThrowsWhenNoNodeCoversTheSlot() {
        // A deliberately incomplete topology - a gap between 100 and 200
        // that no node covers, simulating a misconfigured cluster.
        Properties properties = new Properties();
        properties.setProperty("cluster.self", "node1");
        properties.setProperty("cluster.nodes", "node1:127.0.0.1:6379:0-100");
        ClusterConfig config = ClusterConfig.fromProperties(properties);

        assertThrows(IllegalStateException.class, () -> config.ownerOf(200));
    }

    @Test
    void isOwnedByMeMatchesWhicheverNodeIsConfiguredAsSelf() {
        // BUG FOUND BY THIS TEST SUITE (worth keeping the story in the
        // comment): an earlier version of this test only built configs for
        // "node1" and "node2", and assumed the test key had to belong to
        // one of those two - but with 3 nodes in the cluster, a key can
        // just as easily belong to node3, in which case BOTH node1's and
        // node2's isOwnedByMe correctly return false, and a check like
        // "exactly one of these two must be true" fails even though
        // nothing is actually wrong. The fix: build a config for ALL THREE
        // nodes and check that ownership agrees across all of them, not
        // just two.
        ClusterConfig configOnNode1 = ClusterConfig.fromProperties(threeNodeConfig("node1"));
        ClusterConfig configOnNode2 = ClusterConfig.fromProperties(threeNodeConfig("node2"));
        ClusterConfig configOnNode3 = ClusterConfig.fromProperties(threeNodeConfig("node3"));

        String key = "some-key";
        ClusterConfig.NodeInfo owner = configOnNode1.ownerOfKey(key);

        boolean node1ThinksItOwnsIt = configOnNode1.isOwnedByMe(key);
        boolean node2ThinksItOwnsIt = configOnNode2.isOwnedByMe(key);
        boolean node3ThinksItOwnsIt = configOnNode3.isOwnedByMe(key);

        // Each node's opinion must match whether IT is the actual owner.
        assertEquals(owner.id().equals("node1"), node1ThinksItOwnsIt);
        assertEquals(owner.id().equals("node2"), node2ThinksItOwnsIt);
        assertEquals(owner.id().equals("node3"), node3ThinksItOwnsIt);

        // Across all three, exactly one should claim ownership - never
        // zero (every key must belong to someone), never more than one
        // (ownership must be unambiguous).
        int ownerCount = (node1ThinksItOwnsIt ? 1 : 0)
                + (node2ThinksItOwnsIt ? 1 : 0)
                + (node3ThinksItOwnsIt ? 1 : 0);
        assertEquals(1, ownerCount);
    }
}
