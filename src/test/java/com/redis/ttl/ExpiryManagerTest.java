package com.redis.ttl;

import com.redis.core.DataStore;
import com.redis.core.RedisValue;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests for ExpiryManager — the ACTIVE expiry sweep.
 *
 * ===== The one thing that makes this test meaningfully different from
 * testing lazy expiry =====
 * DataStore.get(key) and DataStore.exists(key) BOTH perform lazy expiry
 * as a side effect of being called — so if this test called get() or
 * exists() to check whether a key is gone, we would be triggering removal
 * OURSELVES, and the test would "pass" even if ExpiryManager did nothing
 * at all. That would prove nothing about ExpiryManager specifically.
 *
 * Instead, we use DataStore.size() to check how many keys are in the map.
 * size() is a RAW count of map entries with no lazy-expiry side effect
 * (see its javadoc in DataStore.java) — so if size() drops to 0 on its
 * own, purely because we waited, with no get()/exists() call from this
 * test in between, that can ONLY be explained by ExpiryManager's
 * background sweep having found and removed the key by itself. That's
 * exactly the behavior we're trying to prove exists.
 */
class ExpiryManagerTest {

    @Test
    void activeSweepRemovesExpiredKeyOnItsOwn() throws InterruptedException {
        DataStore dataStore = new DataStore();

        // A key that expires almost immediately (20ms from now).
        RedisValue value = RedisValue.ofString("bar");
        value.setExpireInMillis(20);
        dataStore.set("foo", value);

        // Confirm it's really in there before the sweep gets a chance to run.
        assertEquals(1, dataStore.size());

        ExpiryManager expiryManager = new ExpiryManager(dataStore);
        try {
            expiryManager.start();

            // ExpiryManager sweeps every 100ms. We wait long enough for the
            // key to have expired (20ms) AND for at least two sweep cycles
            // to have run (well past 200ms), giving the background thread
            // a comfortable margin to have actually executed — timing-based
            // tests like this need slack, since exact scheduling isn't
            // guaranteed to the millisecond.
            Thread.sleep(350);

            // We never called get()/exists() above, so this can only be 0
            // because ExpiryManager's own background sweep removed it.
            assertEquals(0, dataStore.size());
        } finally {
            // Always stop the background thread, even if an assertion
            // above fails, so we never leak a running sweep thread across
            // test runs (it's a daemon thread, so it wouldn't hang the JVM,
            // but leaving it running pointlessly is still sloppy).
            expiryManager.stop();
        }
    }

    @Test
    void activeSweepLeavesNonExpiredKeysAlone() throws InterruptedException {
        DataStore dataStore = new DataStore();

        // No expiry set at all (default expireAt is -1, meaning "never").
        dataStore.set("permanent", RedisValue.ofString("stays-forever"));

        ExpiryManager expiryManager = new ExpiryManager(dataStore);
        try {
            expiryManager.start();

            // Give the sweep plenty of chances to run.
            Thread.sleep(350);

            // A key with no expiry should never be touched by the sweep.
            assertEquals(1, dataStore.size());
        } finally {
            expiryManager.stop();
        }
    }
}
