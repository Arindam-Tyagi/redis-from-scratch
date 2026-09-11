package com.redis.persistence;

import com.redis.core.DataStore;
import com.redis.core.RedisValue;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SnapshotManagerTest {

    @TempDir
    Path tempDir;

    @Test
    void saveThenLoadIntoAFreshDataStoreRestoresEveryTypeAndExpiry() throws IOException {
        DataStore original = new DataStore();

        original.set("greeting", RedisValue.ofString("hello"));
        original.set("mylist", RedisValue.ofList(new LinkedList<>(List.of("a", "b", "c"))));
        original.set("myhash", RedisValue.ofHash(new HashMap<>(Map.of("field1", "value1", "field2", "value2"))));
        original.set("myset", RedisValue.ofSet(new LinkedHashSet<>(List.of("m1", "m2", "m3"))));

        // A key WITH a TTL — we capture the exact absolute expiry timestamp
        // here so we can assert the round-tripped value has that EXACT same
        // instant, not just "some expiry or other".
        RedisValue expiringValue = RedisValue.ofString("will expire later");
        expiringValue.setExpireInMillis(600_000); // 10 minutes from now
        long expectedExpireAt = expiringValue.getExpireAt();
        original.set("sessionToken", expiringValue);

        Path walPath = tempDir.resolve("test.wal");
        Path snapshotPath = tempDir.resolve("test.snapshot");
        WriteAheadLog wal = new WriteAheadLog(walPath);

        // Append something to the WAL BEFORE snapshotting, specifically so
        // we can prove the snapshot process truncates it afterward — if
        // truncate() were broken or never called, this assertion at the
        // end of the test would catch it.
        wal.append(List.of("SET", "greeting", "hello"));
        assertTrue(wal.sizeInBytes() > 0, "sanity check: WAL should have bytes before snapshotting");

        SnapshotManager snapshotManager = new SnapshotManager(snapshotPath);
        snapshotManager.save(original, wal);

        // ===== Proof #1: the WAL was truncated after a successful snapshot =====
        assertEquals(0, wal.sizeInBytes());

        // ===== Proof #2: a completely FRESH DataStore, loaded from the =====
        // ===== snapshot file alone, ends up with identical contents =====
        DataStore restored = new DataStore();
        snapshotManager.loadInto(restored);

        assertEquals("hello", restored.get("greeting").orElseThrow().asString());
        assertEquals(List.of("a", "b", "c"), restored.get("mylist").orElseThrow().asList());
        assertEquals(Map.of("field1", "value1", "field2", "value2"), restored.get("myhash").orElseThrow().asHash());
        assertEquals(Set.of("m1", "m2", "m3"), restored.get("myset").orElseThrow().asSet());

        RedisValue restoredExpiring = restored.get("sessionToken").orElseThrow();
        assertEquals("will expire later", restoredExpiring.asString());
        assertTrue(restoredExpiring.hasExpiry());
        assertEquals(expectedExpireAt, restoredExpiring.getExpireAt());

        wal.close();
    }

    @Test
    void alreadyExpiredKeysAreNotIncludedInTheSnapshotAtAll() throws IOException {
        DataStore original = new DataStore();

        RedisValue longGone = RedisValue.ofString("should not survive");
        // An expiry timestamp already in the PAST — this key is expired
        // right now, at the moment we snapshot it.
        longGone.setExpireAtTimestamp(System.currentTimeMillis() - 10_000);
        original.set("staleKey", longGone);
        original.set("freshKey", RedisValue.ofString("still alive"));

        Path walPath = tempDir.resolve("test.wal");
        Path snapshotPath = tempDir.resolve("test.snapshot");
        WriteAheadLog wal = new WriteAheadLog(walPath);
        SnapshotManager snapshotManager = new SnapshotManager(snapshotPath);

        snapshotManager.save(original, wal);

        DataStore restored = new DataStore();
        snapshotManager.loadInto(restored);

        assertFalse(restored.exists("staleKey"), "an already-expired key should never be written into the snapshot");
        assertTrue(restored.exists("freshKey"));

        wal.close();
    }

    @Test
    void loadingWhenNoSnapshotFileExistsYetLeavesDataStoreEmptyWithoutError() throws IOException {
        Path snapshotPath = tempDir.resolve("never-created.snapshot");
        SnapshotManager snapshotManager = new SnapshotManager(snapshotPath);

        DataStore dataStore = new DataStore();
        // Should simply do nothing — no exception — since this exact
        // situation (a node's very first-ever startup, before any snapshot
        // has ever been taken) is a normal, expected case.
        snapshotManager.loadInto(dataStore);

        assertEquals(0, dataStore.size());
    }
}
